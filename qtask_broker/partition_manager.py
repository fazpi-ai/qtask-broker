import hashlib
import logging
# Import the configuration class
from config import ConfigurationLoader

logger = logging.getLogger(__name__)

class PartitionManager:
    """
    Manages partitioning logic, including calculating the partition index
    based on a key and generating partitioned stream names.
    """
    def __init__(self, config: ConfigurationLoader):
        """
        Initializes the PartitionManager.

        Args:
            config: A ConfigurationLoader instance with the loaded configuration.
        """
        self.config = config
        # Validate num_partitions once at the beginning
        if not isinstance(config.num_partitions, int) or config.num_partitions <= 0:
            logger.error(f"Invalid number of partitions in configuration: {config.num_partitions}. Must be an integer > 0.")
            # You could raise an exception here or use a safe default,
            # but raising an exception is more explicit about a configuration problem.
            raise ValueError(f"Invalid configuration: NUM_PARTITIONS must be a positive integer.")
        self.num_partitions = config.num_partitions
        logger.info(f"PartitionManager initialized with {self.num_partitions} partitions.")

    def get_partition_index(self, partition_key: str) -> int:
        """
        Calculates the partition index for a given key using SHA-1.

        Args:
            partition_key: The key (string) used to determine the partition.

        Returns:
            The partition index (an integer between 0 and num_partitions-1).
            Returns 0 if the key is None or empty.
        """
        if not partition_key:
            logger.warning("Received an empty or None partition_key. Assigning to partition 0.")
            return 0

        try:
            # 1. Create SHA-1 hash of the string (encoded to bytes)
            sha1_hash = hashlib.sha1(partition_key.encode('utf-8')).digest()
            # 2. Read the first 4 bytes of the hash as an unsigned big-endian integer
            #    (Equivalent to hash.readUInt32BE(0) in Node.js)
            hash_int = int.from_bytes(sha1_hash[:4], byteorder='big', signed=False)
            # 3. Calculate the index using the modulo (%) operator
            index = hash_int % self.num_partitions
            logger.debug(f"Key '{partition_key}' hashed to index {index} (out of {self.num_partitions} partitions)")
            return index
        except Exception as e:
            # Catch unexpected errors during hashing
            logger.error(f"Error calculating partition index for key '{partition_key}': {e}", exc_info=True)
            # Return 0 as a safe fallback in case of unexpected errors
            return 0

    def get_partitioned_stream_key(self, base_topic: str, partition_index: int) -> str:
        """
        Generates the full stream key name for a specific base topic and
        partition index.

        Args:
            base_topic: The base topic name (e.g., 'orders', 'events').
            partition_index: The numerical index of the partition.

        Returns:
            The complete stream key (e.g., 'stream:orders:5').

        Raises:
            ValueError: If the partition_index is outside the valid range (0 to num_partitions-1).
        """
        if not isinstance(partition_index, int) or not (0 <= partition_index < self.num_partitions):
            error_msg = f"Invalid partition index: {partition_index}. Must be an integer between 0 and {self.num_partitions - 1}."
            logger.error(error_msg)
            raise ValueError(error_msg)

        stream_key = f"stream:{base_topic}:{partition_index}"
        logger.debug(f"Generated stream key: '{stream_key}' for topic '{base_topic}', partition {partition_index}")
        return stream_key

# --- Example Usage ---
if __name__ == "__main__":
    # Configure basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # 1. Load configuration (assumes you have .env or defined variables)
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        exit(1) # Exit if configuration is invalid

    # 2. Create PartitionManager instance
    partition_manager = PartitionManager(config)

    # 3. Test get_partition_index
    print("\n--- Testing get_partition_index ---")
    key1 = "user-123"
    key2 = "order-abc"
    key3 = "user-123" # Same key as key1
    key_empty = ""

    index1 = partition_manager.get_partition_index(key1)
    index2 = partition_manager.get_partition_index(key2)
    index3 = partition_manager.get_partition_index(key3)
    index_empty = partition_manager.get_partition_index(key_empty)

    print(f"Index for '{key1}': {index1}")
    print(f"Index for '{key2}': {index2}")
    print(f"Index for '{key3}': {index3} (Should be equal to {index1})")
    print(f"Index for empty key: {index_empty} (Should be 0)")

    # 4. Test get_partitioned_stream_key
    print("\n--- Testing get_partitioned_stream_key ---")
    topic = "notifications"
    valid_index = index1 # Use a valid index calculated earlier
    invalid_index_neg = -1
    invalid_index_high = config.num_partitions # Out of range (0 to N-1)

    try:
        stream_key_valid = partition_manager.get_partitioned_stream_key(topic, valid_index)
        print(f"Stream key for '{topic}', partition {valid_index}: '{stream_key_valid}'")
    except ValueError as e:
        print(f"Unexpected error generating valid key: {e}")

    try:
        partition_manager.get_partitioned_stream_key(topic, invalid_index_neg)
    except ValueError as e:
        print(f"Expected error generating key with index {invalid_index_neg}: {e}")

    try:
        partition_manager.get_partitioned_stream_key(topic, invalid_index_high)
    except ValueError as e:
        print(f"Expected error generating key with index {invalid_index_high}: {e}")

