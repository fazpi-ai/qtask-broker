import json
import logging
# Import necessary dependencies
from redis_manager import RedisManager
from partition_manager import PartitionManager
# Import the constant from SubscriptionService if you don't want to repeat it
# Assuming subscription_service.py also exists in the same directory
from subscription_service import SubscriptionService
# Import ConfigurationLoader for type hinting if needed
from config import ConfigurationLoader

logger = logging.getLogger(__name__)

class PublishingService:
    """
    Manages the publication of messages to the correct partitions of topics,
    ensuring serialization and registration of the base topic.
    """
    MANAGED_TOPICS_KEY = SubscriptionService.MANAGED_TOPICS_KEY # Reuse constant

    def __init__(self, redis_manager: RedisManager, partition_manager: PartitionManager):
        """
        Initializes the PublishingService.

        Args:
            redis_manager: A RedisManager instance to interact with Redis.
            partition_manager: A PartitionManager instance to calculate partitions.
        """
        self.redis_manager = redis_manager
        self.partition_manager = partition_manager
        logger.info("PublishingService initialized.")

    def publish_message(self, topic: str, partition_key: str, data: dict) -> tuple[bool, int | None, str | None]:
        """
        Publishes a message to the appropriate partition of a topic.

        Args:
            topic: The base topic name.
            partition_key: The key used to determine the partition.
            data: The Python dictionary containing the message data to publish.

        Returns:
            A tuple (success: bool, partition_index: int | None, message_id: str | None):
            - success: True if the message was published and the topic registered, False otherwise.
            - partition_index: The index of the partition where it was published, or None if failed before.
            - message_id: The message ID returned by Redis (XADD), or None if failed.
        """
        logger.info(f"Attempting to publish message to topic '{topic}' with partition key '{partition_key}'.")

        # 1. Determine the target partition
        try:
            target_partition = self.partition_manager.get_partition_index(partition_key)
            logger.debug(f"Key '{partition_key}' mapped to partition {target_partition}.")
        except Exception as e:
            # Although get_partition_index has its own try-except, add one here just in case.
            logger.error(f"Unexpected error calculating partition for key '{partition_key}': {e}", exc_info=True)
            return False, None, None

        # 2. Get the partitioned stream key
        try:
            stream_key = self.partition_manager.get_partitioned_stream_key(topic, target_partition)
            logger.debug(f"Target stream key: '{stream_key}'.")
        except ValueError as e:
            # This shouldn't happen if get_partition_index worked, but for safety.
            logger.error(f"Error generating stream key for partition {target_partition}: {e}")
            return False, target_partition, None

        # 3. Serialize the data to JSON
        try:
            # We save the payload as a JSON string in a field named 'payload'
            # (similar to your original JS code)
            message_payload = {'payload': json.dumps(data)}
        except (TypeError, OverflowError) as e:
            logger.error(f"Error serializing data to JSON for topic '{topic}': {e}", exc_info=True)
            return False, target_partition, None

        # 4. Publish the message using RedisManager (XADD)
        message_id = self.redis_manager.xadd(stream_key, message_payload)

        if message_id is None:
            # RedisManager.xadd already logged the error (connection, etc.)
            logger.error(f"Failed to publish message (XADD) to stream '{stream_key}'.")
            return False, target_partition, None

        logger.info(f"Message published successfully to '{stream_key}' with ID: {message_id}.")

        # 5. Register the base topic (SADD)
        sadd_result = self.redis_manager.sadd(self.MANAGED_TOPICS_KEY, topic)

        if sadd_result is None:
            # RedisManager.sadd already logged the error
            logger.warning(f"Failed to register base topic '{topic}' in set '{self.MANAGED_TOPICS_KEY}' (message WAS published).")
            # We consider the publish successful, but registration failed.
            # You might decide to return False here if registration is critical.
            # For now, return True because the message was sent.
            return True, target_partition, message_id
        elif sadd_result == 1:
            logger.info(f"Base topic '{topic}' added to set '{self.MANAGED_TOPICS_KEY}'.")
        # else: sadd_result == 0 (already existed, no need to log again)

        return True, target_partition, message_id

# --- Example Usage ---
if __name__ == "__main__":
    # Configure basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # Need these for the example
    from config import ConfigurationLoader
    from redis_manager import RedisManager
    from partition_manager import PartitionManager
    from subscription_service import SubscriptionService # Needed for MANAGED_TOPICS_KEY

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

    # 3. Create PublishingService
    publishing_service = PublishingService(redis_manager, partition_manager)

    # 4. Test publishing (requires Redis connection)
    logger.info("\n--- Testing PublishingService ---")
    topic = "user_activity"
    key1 = "user:1001"
    data1 = {"action": "login", "timestamp": "2025-04-27T14:00:00Z"}

    key2 = "user:2050"
    data2 = {"action": "view_page", "url": "/products/123"}

    data_invalid_json = {"set_object": {1, 2, 3}} # Sets are not JSON serializable

    # Ensure initial connection (optional)
    if not redis_manager._ensure_connection():
        logger.error("Could not connect to Redis for the example.")
        exit(1)

    print(f"\nPublishing message 1 (key: {key1})...")
    success1, part1, id1 = publishing_service.publish_message(topic, key1, data1)
    print(f"Result 1: Success={success1}, Partition={part1}, ID={id1}")

    print(f"\nPublishing message 2 (key: {key2})...")
    success2, part2, id2 = publishing_service.publish_message(topic, key2, data2)
    print(f"Result 2: Success={success2}, Partition={part2}, ID={id2}")
    if part1 is not None and part2 is not None:
         print(f"(Note: Partition {part1} for {key1} and {part2} for {key2} may be the same or different)")

    print(f"\nAttempting to publish non-serializable data...")
    success_err, part_err, id_err = publishing_service.publish_message(topic, key1, data_invalid_json)
    print(f"JSON Error Result (should be False): Success={success_err}, Partition={part_err}, ID={id_err}")


    # Verify the set of managed topics (optional)
    if redis_manager.redis_client:
        managed_topics = redis_manager.smembers(PublishingService.MANAGED_TOPICS_KEY)
        print(f"\nManaged topics in Redis: {managed_topics}") # Should include 'user_activity'

    # Close connection
    redis_manager.close_connection()
