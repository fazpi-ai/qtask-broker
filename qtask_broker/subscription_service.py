import logging
# Import necessary dependencies
from qtask_broker.redis_manager import RedisManager
from qtask_broker.partition_manager import PartitionManager
# Import ConfigurationLoader for type hinting if needed, although not used directly
from qtask_broker.config import ConfigurationLoader

logger = logging.getLogger(__name__)

class SubscriptionService:
    """
    Manages group subscriptions to topic partitions, ensuring
    the existence of the stream and group in Redis, and registering the base topic.
    """
    MANAGED_TOPICS_KEY = 'managed_topics' # Key for the set in Redis

    def __init__(self, redis_manager: RedisManager, partition_manager: PartitionManager):
        """
        Initializes the SubscriptionService.

        Args:
            redis_manager: A RedisManager instance to interact with Redis.
            partition_manager: A PartitionManager instance to generate stream keys.
        """
        self.redis_manager = redis_manager
        self.partition_manager = partition_manager
        logger.info("SubscriptionService initialized.")

    def ensure_subscription(self, topic: str, group: str, partition_index: int) -> bool:
        """
        Ensures that a group is subscribed to a specific partition of a topic.

        This involves:
        1. Getting the partitioned stream key.
        2. Attempting to create the group for that stream (using MKSTREAM to create the stream if it doesn't exist).
        3. Adding the base topic to the set of managed topics.

        Handles the 'BUSYGROUP' case (group already exists) as success.

        Args:
            topic: The base topic name.
            group: The consumer group name.
            partition_index: The numerical index of the partition.

        Returns:
            True if the subscription was ensured (or already existed), False if an
            unrecoverable error occurred (e.g., connection error, error adding to set).
        """
        logger.info(f"Ensuring subscription for group '{group}' on partition {partition_index} of topic '{topic}'.")

        # 1. Get the partitioned stream key
        try:
            stream_key = self.partition_manager.get_partitioned_stream_key(topic, partition_index)
            logger.debug(f"Partitioned stream key obtained: '{stream_key}'")
        except ValueError as e:
            # The partition index was invalid according to PartitionManager
            logger.error(f"Cannot ensure subscription: {e}")
            return False

        # 2. Attempt to create the group and the stream (if they don't exist)
        # RedisManager.xgroup_create internally handles BUSYGROUP as success (returns True)
        # and also handles connection errors.
        group_created_or_exists = self.redis_manager.xgroup_create(
            stream_key=stream_key,
            group_name=group,
            mkstream=True # Important! Creates the stream if it doesn't exist
        )

        if not group_created_or_exists:
            # An error other than BUSYGROUP occurred, or an unrecoverable connection error
            logger.error(f"Critical failure attempting to create/verify group '{group}' on stream '{stream_key}'.")
            return False # Could not ensure the group

        # 3. Add the base topic to the set of managed topics
        # We use SADD, which is idempotent (doesn't add duplicates).
        sadd_result = self.redis_manager.sadd(self.MANAGED_TOPICS_KEY, topic)

        if sadd_result is None:
            # A connection error or similar occurred during SADD
            logger.error(f"Critical failure attempting to add base topic '{topic}' to set '{self.MANAGED_TOPICS_KEY}'.")
            return False # The subscription is not fully ensured

        if sadd_result == 1:
            logger.info(f"Base topic '{topic}' added to set '{self.MANAGED_TOPICS_KEY}'.")
        else: # sadd_result == 0
            logger.info(f"Base topic '{topic}' already existed in set '{self.MANAGED_TOPICS_KEY}'.")

        # If we reached here, both the group/stream and the topic registration are ensured
        logger.info(f"Subscription for group '{group}' on partition {partition_index} of topic '{topic}' ensured successfully.")
        return True

# --- Example Usage ---
if __name__ == "__main__":
    # Configure basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # Need these for the example
    from config import ConfigurationLoader
    from redis_manager import RedisManager
    from partition_manager import PartitionManager

    # 1. Load configuration
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        exit(1)

    # 2. Create dependencies
    try:
        redis_manager = RedisManager(config)
        partition_manager = PartitionManager(config)
    except ValueError as e: # Catch init errors from managers too
         logger.error(f"Error initializing managers: {e}")
         exit(1)


    # 3. Create SubscriptionService
    subscription_service = SubscriptionService(redis_manager, partition_manager)

    # 4. Test ensuring subscriptions (requires Redis connection)
    logger.info("\n--- Testing SubscriptionService ---")
    topic1 = "orders"
    group1 = "order_processors"
    partition_idx1 = 0
    partition_idx2 = 1

    topic2 = "inventory_updates"
    group2 = "stock_alerters"
    partition_idx3 = 0

    # Ensure initial connection (optional)
    if not redis_manager._ensure_connection():
        logger.error("Could not connect to Redis for the example.")
        exit(1)

    print(f"\nEnsuring subscription for {topic1}/{group1} on partition {partition_idx1}...")
    success1 = subscription_service.ensure_subscription(topic1, group1, partition_idx1)
    print(f"Result 1: {success1}")

    print(f"\nEnsuring subscription again for {topic1}/{group1} on partition {partition_idx1}...")
    success2 = subscription_service.ensure_subscription(topic1, group1, partition_idx1)
    print(f"Result 2 (should be True): {success2}")

    print(f"\nEnsuring subscription for {topic1}/{group1} on partition {partition_idx2}...")
    success3 = subscription_service.ensure_subscription(topic1, group1, partition_idx2)
    print(f"Result 3: {success3}")

    print(f"\nEnsuring subscription for {topic2}/{group2} on partition {partition_idx3}...")
    success4 = subscription_service.ensure_subscription(topic2, group2, partition_idx3)
    print(f"Result 4: {success4}")

    print(f"\nAttempting to ensure subscription with invalid index...")
    success_invalid = subscription_service.ensure_subscription(topic1, group1, config.num_partitions) # Index out of range
    print(f"Invalid index result (should be False): {success_invalid}")


    # Verify the set of managed topics (optional - requires direct access)
    if redis_manager.redis_client:
        managed_topics = redis_manager.smembers(SubscriptionService.MANAGED_TOPICS_KEY)
        print(f"\nManaged topics in Redis: {managed_topics}") # Should include 'orders' and 'inventory_updates'

    # Close connection
    redis_manager.close_connection()
