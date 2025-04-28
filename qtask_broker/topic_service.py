import logging
from typing import List, Set, Optional # For more specific type hints

# Import necessary dependencies
from qtask_broker.redis_manager import RedisManager
# Import the constant from SubscriptionService or define it here
# Assuming subscription_service.py also exists
from qtask_broker.subscription_service import SubscriptionService
# Import ConfigurationLoader for type hinting if needed
from qtask_broker.config import ConfigurationLoader


logger = logging.getLogger(__name__)

class TopicService:
    """
    Provides functionality to retrieve the list of managed base topics
    from Redis.
    """
    MANAGED_TOPICS_KEY = SubscriptionService.MANAGED_TOPICS_KEY # Reuse the constant

    def __init__(self, redis_manager: RedisManager):
        """
        Initializes the TopicService.

        Args:
            redis_manager: A RedisManager instance to interact with Redis.
        """
        self.redis_manager = redis_manager
        logger.info("TopicService initialized.")

    def get_managed_topics(self) -> List[str]:
        """
        Gets the list of all base topics registered in the 'managed_topics'
        set in Redis.

        Returns:
            A list of strings with the base topic names.
            Returns an empty list if there are no topics or if an error occurs
            communicating with Redis.
        """
        logger.info(f"Getting list of managed topics from key '{self.MANAGED_TOPICS_KEY}'...")

        # Call smembers via RedisManager
        # redis_manager.smembers returns a set or None in case of error
        topics_set: Optional[Set[str]] = self.redis_manager.smembers(self.MANAGED_TOPICS_KEY)

        if topics_set is None:
            # An error occurred (e.g., connection), RedisManager should have logged it.
            logger.error(f"Could not retrieve topic list from Redis (key: {self.MANAGED_TOPICS_KEY}).")
            return [] # Return empty list on error

        # Convert the set to a list for a more standard response (optional)
        topics_list = sorted(list(topics_set)) # Sort alphabetically
        logger.info(f"Found {len(topics_list)} managed topics.")
        logger.debug(f"Topics found: {topics_list}")

        return topics_list

# --- Example Usage ---
if __name__ == "__main__":
    # Configure basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # Import necessary classes for the example
    from config import ConfigurationLoader
    from redis_manager import RedisManager
    # Need SubscriptionService just for the key constant in this example context
    from subscription_service import SubscriptionService


    # 1. Load configuration
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        exit(1)

    # 2. Create RedisManager
    try:
        redis_manager = RedisManager(config)
    except ValueError as e:
         logger.error(f"Error initializing RedisManager: {e}")
         exit(1)


    # 3. Create TopicService
    topic_service = TopicService(redis_manager)

    # 4. Get topics (requires Redis connection)
    logger.info("\n--- Testing TopicService ---")

    # Ensure initial connection (optional)
    if not redis_manager._ensure_connection():
        logger.error("Could not connect to Redis for the example.")
        exit(1)

    # (Optional) Add some example topics if Redis is empty
    print("\n(Ensuring some example topics exist...)")
    redis_manager.sadd(TopicService.MANAGED_TOPICS_KEY, "example_topic_1")
    redis_manager.sadd(TopicService.MANAGED_TOPICS_KEY, "example_topic_2")
    redis_manager.sadd(TopicService.MANAGED_TOPICS_KEY, "another_one")


    # Get the list
    print("\nGetting list of topics...")
    topic_list = topic_service.get_managed_topics() # Renamed variable

    if topic_list:
        print("\nManaged topics found:")
        for topic in topic_list:
            print(f"- {topic}")
    else:
        print("\nNo managed topics found or an error occurred.")

    # Close connection
    redis_manager.close_connection()
