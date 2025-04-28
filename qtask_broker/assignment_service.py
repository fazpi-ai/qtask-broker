import logging
# Import necessary dependencies
from qtask_broker.redis_manager import RedisManager
from qtask_broker.config import ConfigurationLoader # Needed indirectly via RedisManager

logger = logging.getLogger(__name__)

class AssignmentService:
    """
    Encapsulates the logic for assigning partitions to consumers
    by executing the Lua script via RedisManager.
    """
    def __init__(self, redis_manager: RedisManager):
        """
        Initializes the AssignmentService.

        Args:
            redis_manager: A RedisManager instance to interact with Redis.
        """
        self.redis_manager = redis_manager
        # Access configuration through the redis_manager
        self.config: ConfigurationLoader = redis_manager.config
        logger.info("AssignmentService initialized.")

    def _get_assignments_key(self, base_topic: str, group: str) -> str:
        """
        Generates the hash key used to store partition -> consumer assignments
        for a specific topic and group.
        (Equivalent to getAssignmentsKey in JS).

        Args:
            base_topic: The base topic name.
            group: The consumer group name.

        Returns:
            The assignments key (e.g., 'assignments:my_topic:my_group').
        """
        key = f"assignments:{base_topic}:{group}"
        logger.debug(f"Generated assignments key: '{key}'")
        return key

    def assign_partition(self, topic: str, group: str, consumer_id: str) -> int:
        """
        Attempts to assign an available partition of the specified topic/group
        to the given consumer by executing the Lua script.

        Args:
            topic: The base topic name.
            group: The consumer group name.
            consumer_id: The unique ID of the consumer requesting the assignment.

        Returns:
            The assigned partition index (an integer >= 0) if the assignment
            was successful.
            -1 if no available partitions were found or if an error occurred
            during script execution (e.g., connection error, script not loaded).
        """
        logger.info(f"Assignment request received for consumer '{consumer_id}' on topic '{topic}', group '{group}'.")

        # 1. Get the key where assignments are stored
        assignments_key = self._get_assignments_key(topic, group)

        # 2. Get necessary parameters from configuration
        num_partitions = self.config.num_partitions
        # heartbeat_prefix is used internally by redis_manager

        # Validate that num_partitions is valid (although already validated in Config and PartitionManager)
        if num_partitions <= 0:
             logger.error(f"Invalid configuration detected in AssignmentService: num_partitions={num_partitions}")
             return -1 # Cannot assign without valid partitions

        # 3. Execute the Lua script via RedisManager
        logger.debug(f"Calling redis_manager.execute_assignment_script with key='{assignments_key}', consumer='{consumer_id}', partitions={num_partitions}")
        assigned_partition = self.redis_manager.execute_assignment_script(
            assignments_key=assignments_key,
            consumer_id=consumer_id,
            num_partitions=num_partitions
            # heartbeat_prefix is used internally by execute_assignment_script
        )

        # 4. Interpret the result
        if assigned_partition >= 0:
            logger.info(f"Lua script successfully assigned partition {assigned_partition} to '{consumer_id}' for '{topic}/{group}'.")
            return assigned_partition
        else:
            # The value -1 can mean "no free partitions" or an internal error
            # RedisManager should have already logged specific errors (connection, script).
            logger.warning(f"Could not assign partition for '{consumer_id}' on '{topic}/{group}'. Script returned {assigned_partition}.")
            return -1

# --- Example Usage ---
if __name__ == "__main__":
    # Configure basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # 1. Load configuration
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        exit(1)

    # 2. Create RedisManager
    redis_manager = RedisManager(config)

    # 3. Create AssignmentService
    assignment_service = AssignmentService(redis_manager)

    # 4. Simulate assignments (requires Redis connection)
    logger.info("\n--- Testing AssignmentService ---")
    topic = "user_events"
    group = "processor_group"
    consumer_a = "worker-alpha"
    consumer_b = "worker-beta"

    # Ensure initial connection (optional, execute_assignment_script will do it)
    if not redis_manager._ensure_connection():
        logger.error("Could not connect to Redis for the example.")
        exit(1)

    # Simulate heartbeats (necessary for Lua to consider them alive)
    try:
        # Need direct client access for this in the example
        if redis_manager.redis_client:
            redis_manager.redis_client.set(f"{config.heartbeat_key_prefix}{consumer_a}", "alive", ex=config.heartbeat_ttl_seconds)
            redis_manager.redis_client.set(f"{config.heartbeat_key_prefix}{consumer_b}", "alive", ex=config.heartbeat_ttl_seconds)
            logger.info("Simulated heartbeats for worker-alpha and worker-beta.")
        else:
             logger.warning("Cannot simulate heartbeats, Redis client not connected.")
    except Exception as e:
        logger.warning(f"Could not simulate heartbeats (Redis unavailable?): {e}")


    print(f"\nAttempting assignment for {consumer_a}...")
    partition_a = assignment_service.assign_partition(topic, group, consumer_a)
    print(f"Result for {consumer_a}: {partition_a}")

    print(f"\nAttempting assignment for {consumer_b}...")
    partition_b = assignment_service.assign_partition(topic, group, consumer_b)
    print(f"Result for {consumer_b}: {partition_b}")

    print(f"\nAttempting assignment again for {consumer_a}...")
    partition_a_again = assignment_service.assign_partition(topic, group, consumer_a)
    print(f"Re-assignment result for {consumer_a}: {partition_a_again} (should be {partition_a})")

    # Clean up assignments (optional - requires direct client access)
    # assignments_key = assignment_service._get_assignments_key(topic, group)
    # if redis_manager.redis_client:
    #     deleted_count = redis_manager.redis_client.delete(assignments_key)
    #     logger.info(f"Assignments key '{assignments_key}' deleted: {deleted_count}")

    # Close connection
    redis_manager.close_connection()
