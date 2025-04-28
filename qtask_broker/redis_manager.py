import redis
import logging
import time
# Import the configuration class we created
from config import ConfigurationLoader

# Configure logger for this module
# It's better to configure logging once in your application's main entry point.
# If you already do it there, you can comment out or remove the following line.
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

# --- The Lua Assignment Script ---
# Defined as a constant for clarity
ASSIGN_PARTITION_LUA_SCRIPT = """
  local assignmentsKey = KEYS[1]
  local requestingConsumerId = ARGV[1]
  local numPartitions = tonumber(ARGV[2])
  local heartbeatPrefix = ARGV[3]

  redis.log(redis.LOG_NOTICE, "Lua Start: reqConsumer=" .. requestingConsumerId .. ", numPartitions=" .. ARGV[2])

  if numPartitions == nil or numPartitions <= 0 then
    redis.log(redis.LOG_WARNING, "Invalid numPartitions: " .. ARGV[2])
    return -1 -- Return -1 to indicate argument error
  end

  local assignments = redis.call('HGETALL', assignmentsKey)
  local assigned_to_requester = -1
  local cleaned_partition = -1
  local current_assignments = {} -- Table to track current assignments

  -- Loop 1: Check existing assignments & heartbeats
  for i = 1, #assignments, 2 do
    local partitionIndexStr = assignments[i]
    local partitionIndexNum = tonumber(partitionIndexStr)
    local assignedConsumerId = assignments[i+1]
    local heartbeatKey = heartbeatPrefix .. assignedConsumerId
    redis.log(redis.LOG_NOTICE, "Lua Loop1: Checking partition " .. partitionIndexStr .. " assigned to " .. assignedConsumerId)

    -- Check if assignment is within the valid partition range
    if partitionIndexNum ~= nil and partitionIndexNum >= 0 and partitionIndexNum < numPartitions then
      current_assignments[partitionIndexStr] = assignedConsumerId -- Register valid assignment
      if assignedConsumerId == requestingConsumerId then
        redis.log(redis.LOG_NOTICE, "Lua Loop1: Partition " .. partitionIndexStr .. " already assigned to requester.")
        if redis.call('EXISTS', heartbeatKey) == 1 then
          redis.log(redis.LOG_NOTICE, "Lua Loop1: Heartbeat OK. Returning partition " .. partitionIndexStr)
          return partitionIndexNum -- Requester already has it and is alive
        else
          redis.log(redis.LOG_WARNING, "Lua Loop1: Heartbeat MISSING for requester on partition " .. partitionIndexStr .. ". Will try HSETNX later.")
          assigned_to_requester = partitionIndexNum -- Mark that requester had it but might be dead
        end
      else
        -- Check heartbeat of the *other* consumer
        if redis.call('EXISTS', heartbeatKey) == 0 then
          redis.log(redis.LOG_WARNING, "Lua Loop1: Heartbeat MISSING for " .. assignedConsumerId .. ". Cleaning partition " .. partitionIndexStr)
          redis.call('HDEL', assignmentsKey, partitionIndexStr)
          current_assignments[partitionIndexStr] = nil -- Mark as free
          cleaned_partition = partitionIndexNum -- Mark this partition as potentially free first
        else
           redis.log(redis.LOG_NOTICE, "Lua Loop1: Partition " .. partitionIndexStr .. " busy by alive consumer " .. assignedConsumerId)
        end
      end
    else
      redis.log(redis.LOG_WARNING, "Lua Loop1: Cleaning assignment for invalid partition index " .. partitionIndexStr .. " (numPartitions=" .. numPartitions .. ")")
      redis.call('HDEL', assignmentsKey, partitionIndexStr) -- Clean up old invalid assignments
    end
  end -- End Loop 1

  -- If a cleaned partition is available, try to assign it first
  if cleaned_partition ~= -1 then
      redis.log(redis.LOG_NOTICE, "Lua PostLoop1: Trying to assign cleaned partition " .. cleaned_partition .. " to " .. requestingConsumerId)
      local reassigned = redis.call('HSETNX', assignmentsKey, tostring(cleaned_partition), requestingConsumerId)
      if reassigned == 1 then
          redis.log(redis.LOG_NOTICE, "Lua PostLoop1: Assigned cleaned partition " .. cleaned_partition)
          return cleaned_partition
      else
           redis.log(redis.LOG_WARNING, "Lua PostLoop1: Cleaned partition " .. cleaned_partition .. " taken by someone else already?")
           -- Update our view of current assignments
           current_assignments[tostring(cleaned_partition)] = redis.call('HGET', assignmentsKey, tostring(cleaned_partition))
           -- Continue to Loop 2 to find another free one
      end
  end

  -- Loop 2: Find a completely free slot or re-confirm requester's slot
  redis.log(redis.LOG_NOTICE, "Lua Starting Loop2: Searching from 0 to " .. (numPartitions - 1))
  for i = 0, numPartitions - 1 do
    local partitionIndexStr = tostring(i)
    -- Only try HSETNX if it's not already assigned (according to our updated view)
    if current_assignments[partitionIndexStr] == nil then
        redis.log(redis.LOG_NOTICE, "Lua Loop2: Trying HSETNX for partition " .. partitionIndexStr)
        local assigned = redis.call('HSETNX', assignmentsKey, partitionIndexStr, requestingConsumerId)
        if assigned == 1 then
          redis.log(redis.LOG_NOTICE, "Lua Loop2: HSETNX SUCCESS. Returning partition " .. i)
          return i -- Assigned a free slot
        else
          -- Slot taken between check and HSETNX, update view and check if it's the requester
          local currentAssignee = redis.call('HGET', assignmentsKey, partitionIndexStr)
          redis.log(redis.LOG_WARNING, "Lua Loop2: HSETNX failed for partition " .. i .. ". Current assignee: " .. (currentAssignee or 'nil'))
          current_assignments[partitionIndexStr] = currentAssignee -- Update view
          if currentAssignee == requestingConsumerId then
             redis.log(redis.LOG_NOTICE, "Lua Loop2: Partition " .. i .. " confirmed for requester (race condition?). Returning " .. i)
             return i
          end
        end
    elseif current_assignments[partitionIndexStr] == requestingConsumerId then
         -- Slot is already assigned to the requester (from Loop 1 or race condition)
         redis.log(redis.LOG_NOTICE, "Lua Loop2: Partition " .. i .. " already belongs to requester. Returning " .. i)
         return i
    else
         redis.log(redis.LOG_NOTICE, "Lua Loop2: Partition " .. i .. " belongs to someone else (" .. current_assignments[partitionIndexStr] .. "). Skipping.")
    end
  end -- End Loop 2

  redis.log(redis.LOG_WARNING, "Lua Script End: No partition assigned or available. Returning -1")
  return -1 -- No partitions available
"""

class RedisManager:
    """
    Manages the connection to Redis and encapsulates necessary operations
    for the broker, including Lua script execution.
    """
    def __init__(self, config: ConfigurationLoader):
        """
        Initializes the RedisManager.

        Args:
            config: A ConfigurationLoader instance with the loaded configuration.
        """
        self.config = config
        self.redis_client: redis.Redis | None = None
        self._lua_script_sha: str | None = None
        self._connect_retry_interval = 5 # Seconds between connection retries

        # Build connection URL (handles optional user/pass)
        redis_url = f"redis://{config.redis_host}:{config.redis_port}"
        if config.redis_username and config.redis_password:
             redis_url = f"redis://{config.redis_username}:{config.redis_password}@{config.redis_host}:{config.redis_port}"
        elif config.redis_username: # Only username, no password
             redis_url = f"redis://{config.redis_username}@{config.redis_host}:{config.redis_port}"

        logger.info(f"Preparing Redis connection with base URL: redis://...@{config.redis_host}:{config.redis_port}")
        self._redis_url = redis_url

    def _ensure_connection(self):
        """Ensures the Redis connection is active, retrying if necessary."""
        # If already connected and ping works, do nothing more
        try:
            if self.redis_client and self.redis_client.ping():
                return True
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
             logger.warning("Redis ping failed, attempting reconnect...")
             self.redis_client = None # Force reconnection
        except Exception as e:
             logger.error(f"Unexpected error during Redis ping: {e}")
             self.redis_client = None # Force reconnection


        logger.info(f"Attempting to connect to Redis at {self._redis_url}...")
        while True:
            try:
                # decode_responses=True is crucial for working with strings
                self.redis_client = redis.from_url(self._redis_url, decode_responses=True)
                self.redis_client.ping() # Verify connection immediately
                logger.info("Successfully connected to Redis.")
                # Try loading the Lua script after connecting
                self._load_lua_script()
                return True # Exit the while loop if everything succeeded

            # --- CORRECTED ORDER ---
            except redis.exceptions.AuthenticationError as e: # Catch specific error FIRST
                 logger.error(f"Authentication error with Redis: {e}. Check REDIS_USERNAME/REDIS_PASSWORD.")
                 logger.error("Will not retry connection due to authentication error.")
                 self.redis_client = None # Ensure client is None
                 return False # Exit loop and method indicating failure
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e: # Catch general error LATER
                logger.error(f"Failed to connect to Redis: {e}. Retrying in {self._connect_retry_interval}s...")
                self.redis_client = None # Reset client on failure
                time.sleep(self._connect_retry_interval) # Wait before retrying
            # --- END CORRECTED ORDER ---
            except Exception as e: # Catch any other unexpected errors
                logger.error(f"Unexpected error during connection: {e}")
                self.redis_client = None
                time.sleep(self._connect_retry_interval) # Wait before retrying


    def _load_lua_script(self):
        """Loads the Lua script into Redis and stores its SHA1 hash."""
        if not self.redis_client:
            logger.warning("Cannot load Lua script, Redis client not connected.")
            return

        logger.info("Loading Lua script into Redis...")
        try:
            # SCRIPT LOAD returns the SHA1 hash of the script
            self._lua_script_sha = self.redis_client.script_load(ASSIGN_PARTITION_LUA_SCRIPT)
            logger.info(f"Lua script loaded successfully. SHA1: {self._lua_script_sha}")
        except redis.exceptions.ResponseError as e:
            logger.error(f"Error loading Lua script into Redis: {e}")
            self._lua_script_sha = None
        except Exception as e: # Catch other potential errors (e.g., ConnectionError if connection drops here)
            logger.error(f"Unexpected error loading Lua script: {e}")
            self._lua_script_sha = None

    def execute_assignment_script(self, assignments_key: str, consumer_id: str, num_partitions: int) -> int:
        """
        Executes the partition assignment Lua script.

        Args:
            assignments_key: The hash key where assignments are stored.
            consumer_id: The ID of the consumer requesting the partition.
            num_partitions: The total number of configured partitions.

        Returns:
            The assigned partition index (>= 0) or -1 if assignment failed
            or an error occurred.
        """
        if not self._ensure_connection() or not self.redis_client:
             logger.error("Cannot execute Lua script, Redis connection unavailable.")
             return -1 # Indicate failure due to connection

        if not self._lua_script_sha:
            logger.warning("Lua script SHA1 hash unavailable. Attempting to load again...")
            # Don't call _ensure_connection again here, it was already done. Just load script.
            self._load_lua_script()
            if not self._lua_script_sha:
                 logger.error("Could not load Lua script after retry. Cannot execute EVALSHA.")
                 return -1 # Indicate failure due to script not loaded

        keys = [assignments_key]
        args = [
            consumer_id,
            str(num_partitions), # Lua expects strings for ARGV
            self.config.heartbeat_key_prefix
        ]

        try:
            logger.debug(f"Executing EVALSHA {self._lua_script_sha} with KEYS={keys} ARGV={args}")
            # Try executing using the SHA1 hash
            result = self.redis_client.evalsha(self._lua_script_sha, len(keys), *keys, *args)
            logger.debug(f"EVALSHA result: {result} (Type: {type(result)})")
            # Lua script returns a number (or nil, which redis-py might convert to None)
            # Ensure None case is handled before int conversion
            return int(result) if result is not None else -1

        except redis.exceptions.NoScriptError:
            logger.warning("NOSCRIPT error, script not in Redis cache. Attempting EVAL...")
            # The script wasn't cached (maybe Redis restarted), use EVAL once
            # Redis will cache it automatically on success.
            try:
                result = self.redis_client.eval(ASSIGN_PARTITION_LUA_SCRIPT, len(keys), *keys, *args)
                logger.info("EVAL executed successfully after NOSCRIPT.")
                # Try reloading the SHA just in case
                self._load_lua_script()
                return int(result) if result is not None else -1
            except Exception as eval_err:
                logger.error(f"Error executing EVAL after NOSCRIPT: {eval_err}")
                return -1 # Failed even with EVAL
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
             logger.error(f"Connection/Timeout error executing Lua script: {e}. Will attempt reconnect on next call.")
             self.redis_client = None # Force reconnect on next call
             return -1
        except Exception as e:
            logger.error(f"Unexpected error executing Lua script: {e}")
            return -1 # Other error type

    # --- Wrapper methods for other Redis commands ---
    # (Ensure they all call _ensure_connection() first)

    def xadd(self, stream_key: str, data: dict) -> str | None:
        """Adds a message to a stream."""
        if not self._ensure_connection() or not self.redis_client: return None
        try:
            # '*' lets Redis generate the message ID
            message_id = self.redis_client.xadd(stream_key, data)
            logger.debug(f"XADD on {stream_key}: ID={message_id}, Data={data}")
            return message_id
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
             logger.error(f"Connection/Timeout error on XADD for stream {stream_key}: {e}. Will attempt reconnect.")
             self.redis_client = None # Force reconnect
             return None
        except Exception as e:
            logger.error(f"Unexpected error on XADD for stream {stream_key}: {e}")
            return None

    def xgroup_create(self, stream_key: str, group_name: str, start_id: str = '$', mkstream: bool = False) -> bool:
        """Creates a consumer group for a stream."""
        if not self._ensure_connection() or not self.redis_client: return False
        try:
            self.redis_client.xgroup_create(stream_key, group_name, id=start_id, mkstream=mkstream)
            logger.info(f"XGROUP CREATE successful for group '{group_name}' on stream '{stream_key}'.")
            return True
        except redis.exceptions.ResponseError as e:
            # It's normal for the group to already exist, not a fatal error.
            if "BUSYGROUP" in str(e):
                logger.warning(f"Group '{group_name}' already exists on stream '{stream_key}'.")
                return True # Consider success if it already exists
            else:
                logger.error(f"Error (ResponseError) on XGROUP CREATE for {group_name} on {stream_key}: {e}")
                return False
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
             logger.error(f"Connection/Timeout error on XGROUP CREATE for {group_name} on {stream_key}: {e}. Will attempt reconnect.")
             self.redis_client = None # Force reconnect
             return False
        except Exception as e:
            logger.error(f"Unexpected error on XGROUP CREATE for {group_name} on {stream_key}: {e}")
            return False

    def sadd(self, set_key: str, member: str) -> int | None:
        """Adds a member to a set."""
        if not self._ensure_connection() or not self.redis_client: return None
        try:
            result = self.redis_client.sadd(set_key, member)
            logger.debug(f"SADD on {set_key}: Member={member}, Result={result}")
            return result # 1 if added, 0 if already existed
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
             logger.error(f"Connection/Timeout error on SADD for set {set_key}: {e}. Will attempt reconnect.")
             self.redis_client = None # Force reconnect
             return None
        except Exception as e:
            logger.error(f"Unexpected error on SADD for set {set_key}: {e}")
            return None

    def smembers(self, set_key: str) -> set[str] | None:
        """Gets all members of a set."""
        if not self._ensure_connection() or not self.redis_client: return None
        try:
            members = self.redis_client.smembers(set_key)
            logger.debug(f"SMEMBERS on {set_key}: Found={len(members)}")
            return members
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
             logger.error(f"Connection/Timeout error on SMEMBERS for set {set_key}: {e}. Will attempt reconnect.")
             self.redis_client = None # Force reconnect
             return None
        except Exception as e:
            logger.error(f"Unexpected error on SMEMBERS for set {set_key}: {e}")
            return None

    def close_connection(self):
        """Closes the Redis connection if it's open."""
        if self.redis_client:
            try:
                self.redis_client.close()
                logger.info("Redis connection closed.")
            except Exception as e:
                # Log error, but ensure client is set to None regardless
                logger.error(f"Error closing Redis connection: {e}")
            finally:
                 self.redis_client = None # Ensure client is marked as closed

# --- Example Usage (Could be in your main application file) ---
# (The if __name__ == "__main__" block remains the same as before)
if __name__ == "__main__":
    # Configure basic logging if not already configured
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # 1. Load configuration
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        exit(1)


    # 2. Create manager instance
    redis_manager = RedisManager(config)

    # 3. Connect and load script (important to do before using commands)
    # The _ensure_connection method is now called internally by other methods,
    # but we can call it once at the start to check the initial connection if desired.
    logger.info("Checking initial Redis connection...")
    if redis_manager._ensure_connection():
        logger.info("Initial connection verified (or established) and script loaded.")

        # 4. Example usage of wrapper methods
        logger.info("\n--- Testing SADD/SMEMBERS ---")
        redis_manager.sadd("managed_topics", "topic1")
        redis_manager.sadd("managed_topics", "topic2")
        topics = redis_manager.smembers("managed_topics")
        print(f"Managed topics: {topics}")

        logger.info("\n--- Testing XGROUP CREATE ---")
        stream_key_p0 = "stream:topic1:0"
        redis_manager.xgroup_create(stream_key_p0, "groupA", mkstream=True)
        redis_manager.xgroup_create(stream_key_p0, "groupA") # Try again

        logger.info("\n--- Testing XADD ---")
        message_id = redis_manager.xadd(stream_key_p0, {"message": "hello partition 0"})
        print(f"Message added to {stream_key_p0}: ID={message_id}")

        logger.info("\n--- Testing Lua Script ---")
        assignments_key = "assignments:topic1:groupA"
        consumer1_id = "consumer-1"
        num_parts = config.num_partitions

        # Simulate heartbeat for consumer1
        # We need direct client access for this in the example,
        # which isn't ideal but serves for demonstration.
        # In the real app, the heartbeat would be done by the consumer worker.
        if redis_manager.redis_client:
             try:
                 redis_manager.redis_client.set(f"{config.heartbeat_key_prefix}{consumer1_id}", "alive", ex=config.heartbeat_ttl_seconds)
                 logger.info(f"Simulated heartbeat for {consumer1_id}")
             except Exception as e:
                 logger.error(f"Error simulating heartbeat for {consumer1_id}: {e}")
        else:
             logger.warning("Cannot simulate heartbeat, client not connected.")


        print(f"\nAttempting to assign partition for {consumer1_id}...")
        partition1 = redis_manager.execute_assignment_script(assignments_key, consumer1_id, num_parts)
        print(f"Partition assigned to {consumer1_id}: {partition1}")

        print(f"\nAttempting to assign partition again for {consumer1_id}...")
        partition1_again = redis_manager.execute_assignment_script(assignments_key, consumer1_id, num_parts)
        print(f"Partition re-assigned to {consumer1_id}: {partition1_again}")

        consumer2_id = "consumer-2"
        print(f"\nAttempting to assign partition for {consumer2_id}...")
        # Simulate heartbeat for consumer2
        if redis_manager.redis_client:
            try:
                redis_manager.redis_client.set(f"{config.heartbeat_key_prefix}{consumer2_id}", "alive", ex=config.heartbeat_ttl_seconds)
                logger.info(f"Simulated heartbeat for {consumer2_id}")
            except Exception as e:
                 logger.error(f"Error simulating heartbeat for {consumer2_id}: {e}")
        else:
             logger.warning("Cannot simulate heartbeat, client not connected.")

        partition2 = redis_manager.execute_assignment_script(assignments_key, consumer2_id, num_parts)
        print(f"Partition assigned to {consumer2_id}: {partition2}")


        # 5. Close connection at the end
        redis_manager.close_connection()
    else:
        logger.error("Could not establish initial Redis connection. Terminating.")

