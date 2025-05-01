# qtask_broker/redis_manager.py
import redis.asyncio as redis_async # Renombrar alias para claridad
import logging
import asyncio
import time

# --- CORRECCIÓN: Importar excepciones desde el paquete 'redis' base ---
from redis.exceptions import (
    ConnectionError as RedisConnectionError, # Usar alias si ConnectionError choca con built-in
    TimeoutError as RedisTimeoutError,       # Usar alias si TimeoutError choca con asyncio.TimeoutError
    ResponseError as RedisResponseError,
    NoScriptError as RedisNoScriptError,
    AuthenticationError as RedisAuthenticationError,
    RedisError # Clase base por si acaso
)

# Importar ConfigurationLoader (sin cambios)
from qtask_broker.config import ConfigurationLoader

logger = logging.getLogger(__name__)

# --- Script Lua (sin cambios) ---
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
""" # cite: 6

class RedisManager:
    """
    Manages the ASYNCHRONOUS connection to Redis and encapsulates necessary operations
    for the broker, including Lua script execution. (Corrected Exception Handling)
    """
    def __init__(self, config: ConfigurationLoader):
        self.config = config
        # Usar el alias renombrado para el cliente async
        self.redis_client: redis_async.Redis | None = None
        self._lua_script_sha: str | None = None
        self._connect_retry_interval = 5
        self._connect_max_retries = 5
        self._connect_timeout = 10
        self._socket_timeout = 10

        # Construir URL (sin cambios)
        redis_url = f"redis://{config.redis_host}:{config.redis_port}"
        if config.redis_username and config.redis_password:
             redis_url = f"redis://{config.redis_username}:{config.redis_password}@{config.redis_host}:{config.redis_port}"
        elif config.redis_username:
             redis_url = f"redis://{config.redis_username}@{config.redis_host}:{config.redis_port}"

        logger.info(f"Preparing ASYNC Redis connection with base URL: redis://...@{config.redis_host}:{config.redis_port}")
        self._redis_url = redis_url

    async def _ensure_connection(self, retries_left=None):
        """Ensures the Redis connection is active, retrying if necessary. ASYNCHRONOUS."""
        if retries_left is None:
            retries_left = self._connect_max_retries

        if self.redis_client:
            try:
                 if await asyncio.wait_for(self.redis_client.ping(), timeout=self._socket_timeout):
                      return True
            # --- CORRECCIÓN: Usar excepciones importadas ---
            except RedisConnectionError:
                 logger.warning("Async Redis ping failed (ConnectionError), attempting reconnect...")
                 await self.close_connection()
            except RedisTimeoutError: # Timeout desde redis-py
                 logger.warning("Async Redis ping failed (TimeoutError from redis-py), attempting reconnect...")
                 await self.close_connection()
            except asyncio.TimeoutError: # Timeout del asyncio.wait_for
                 logger.warning("Async Redis ping timed out locally (asyncio.TimeoutError), attempting reconnect...")
                 await self.close_connection()
            except RedisAuthenticationError: # Posible si las credenciales cambian
                 logger.error("Authentication error during async Redis ping. Check credentials.")
                 await self.close_connection()
                 return False # No reintentar en auth error
            except Exception as e:
                 logger.error(f"Unexpected error during async Redis ping: {e}", exc_info=True)
                 await self.close_connection()

        if retries_left <= 0:
             logger.error("Max connection retries reached. Could not connect to Redis.")
             return False

        logger.info(f"Attempting to connect ASYNCHRONOUSLY to Redis at {self._redis_url}... ({retries_left} retries left)")
        try:
            # Usar el alias renombrado aquí también
            self.redis_client = redis_async.from_url(
                self._redis_url,
                decode_responses=True,
                socket_connect_timeout=self._connect_timeout,
                socket_timeout=self._socket_timeout
            )
            if not await asyncio.wait_for(self.redis_client.ping(), timeout=self._connect_timeout):
                 raise RedisConnectionError("Ping returned False after connection")

            logger.info("Successfully connected ASYNCHRONOUSLY to Redis.")
            await self._load_lua_script()
            return True

        # --- CORRECCIÓN: Usar excepciones importadas ---
        except RedisAuthenticationError as e:
             logger.error(f"Authentication error with Redis: {e}. Check REDIS_USERNAME/REDIS_PASSWORD.")
             logger.error("Will not retry connection due to authentication error.")
             await self.close_connection()
             return False
        except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
            logger.error(f"Failed to connect to Redis: {e}. Retrying in {self._connect_retry_interval}s...")
            await self.close_connection()
            await asyncio.sleep(self._connect_retry_interval)
            return await self._ensure_connection(retries_left - 1)
        except Exception as e:
            logger.error(f"Unexpected error during async connection: {e}", exc_info=True)
            await self.close_connection()
            await asyncio.sleep(self._connect_retry_interval)
            return await self._ensure_connection(retries_left - 1)

    async def _load_lua_script(self):
        """Loads the Lua script into Redis ASYNCHRONOUSLY."""
        if not self.redis_client:
            logger.warning("Cannot load Lua script, async Redis client not connected.")
            return

        logger.info("Loading/Reloading Lua script into Redis ASYNCHRONOUSLY...")
        try:
            self._lua_script_sha = await self.redis_client.script_load(ASSIGN_PARTITION_LUA_SCRIPT)
            logger.info(f"Lua script loaded successfully (async). SHA1: {self._lua_script_sha}")
        # --- CORRECCIÓN: Usar excepciones importadas ---
        except RedisResponseError as e:
            logger.error(f"Error loading Lua script into Redis (async): {e}")
            self._lua_script_sha = None
        except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
             logger.error(f"Connection/Timeout error loading Lua script (async): {e}")
             self._lua_script_sha = None
             await self.close_connection()
        except Exception as e:
            logger.error(f"Unexpected error loading Lua script (async): {e}", exc_info=True)
            self._lua_script_sha = None

    async def execute_assignment_script(self, assignments_key: str, consumer_id: str, num_partitions: int) -> int:
        """Executes the partition assignment Lua script ASYNCHRONOUSLY."""
        if not await self._ensure_connection() or not self.redis_client:
             logger.error("Cannot execute Lua script (async), Redis connection unavailable.")
             return -1

        if not self._lua_script_sha:
            logger.warning("Lua script SHA1 hash unavailable (async). Attempting to load again...")
            await self._load_lua_script()
            if not self._lua_script_sha:
                 logger.error("Could not load Lua script after retry (async). Cannot execute.")
                 return -1

        keys = [assignments_key]
        args = [consumer_id, str(num_partitions), self.config.heartbeat_key_prefix]

        try:
            logger.debug(f"Executing EVALSHA (async) {self._lua_script_sha} with KEYS={keys} ARGV={args}")
            result = await self.redis_client.evalsha(self._lua_script_sha, len(keys), *keys, *args)
            logger.debug(f"EVALSHA result (async): {result}")
            return int(result) if result is not None else -1
        # --- CORRECCIÓN: Usar excepciones importadas ---
        except RedisNoScriptError:
            logger.warning("NOSCRIPT error (async), script not in Redis cache. Attempting EVAL...")
            try:
                result = await self.redis_client.eval(ASSIGN_PARTITION_LUA_SCRIPT, len(keys), *keys, *args)
                logger.info("EVAL executed successfully after NOSCRIPT (async). Redis will cache it now.")
                # await self._load_lua_script() # Opcional
                return int(result) if result is not None else -1
            except Exception as eval_err:
                logger.error(f"Error executing EVAL after NOSCRIPT (async): {eval_err}", exc_info=True)
                return -1
        # --- CORRECCIÓN: Usar excepciones importadas ---
        except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
             logger.error(f"Connection/Timeout error executing Lua script (async): {e}. Will attempt reconnect on next call.")
             await self.close_connection()
             return -1
        except Exception as e:
            logger.error(f"Unexpected error executing Lua script (async): {e}", exc_info=True)
            return -1

    # --- Wrappers para otros comandos Redis (corregidos) ---

    async def xadd(self, stream_key: str, data: dict) -> str | None:
        """Adds a message to a stream ASYNCHRONOUSLY."""
        if not await self._ensure_connection() or not self.redis_client: return None
        try:
            message_id = await self.redis_client.xadd(stream_key, data)
            logger.debug(f"XADD (async) on {stream_key}: ID={message_id}")
            return message_id
        # --- CORRECCIÓN: Usar excepciones importadas ---
        except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
             logger.error(f"Connection/Timeout error on XADD (async) for {stream_key}: {e}. Will attempt reconnect.")
             await self.close_connection()
             return None
        except Exception as e:
            logger.error(f"Unexpected error on XADD (async) for {stream_key}: {e}", exc_info=True)
            return None

    async def xgroup_create(self, stream_key: str, group_name: str, start_id: str = '$', mkstream: bool = False) -> bool:
        """Creates a consumer group for a stream ASYNCHRONOUSLY."""
        if not await self._ensure_connection() or not self.redis_client: return False
        try:
            await self.redis_client.xgroup_create(stream_key, group_name, id=start_id, mkstream=mkstream)
            logger.info(f"XGROUP CREATE (async) successful for group '{group_name}' on stream '{stream_key}'.")
            return True
        # --- CORRECCIÓN: Usar excepción importada ---
        except RedisResponseError as e: # <--- AQUÍ ESTABA EL ERROR
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                logger.info(f"Group '{group_name}' already exists (async) on stream '{stream_key}'. Considered success.")
                return True
            else:
                logger.error(f"Error (ResponseError) on XGROUP CREATE (async) for {group_name} on {stream_key}: {e}")
                return False
        # --- CORRECCIÓN: Usar excepciones importadas ---
        except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
             logger.error(f"Connection/Timeout error on XGROUP CREATE (async) for {group_name} on {stream_key}: {e}. Will attempt reconnect.")
             await self.close_connection()
             return False
        except Exception as e:
            logger.error(f"Unexpected error on XGROUP CREATE (async) for {group_name} on {stream_key}: {e}", exc_info=True)
            return False

    async def sadd(self, set_key: str, member: str) -> int | None:
        """Adds a member to a set ASYNCHRONOUSLY."""
        if not await self._ensure_connection() or not self.redis_client: return None
        try:
            result = await self.redis_client.sadd(set_key, member)
            logger.debug(f"SADD (async) on {set_key}: Member={member}, Result={result}")
            return result
        # --- CORRECCIÓN: Usar excepciones importadas ---
        except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
             logger.error(f"Connection/Timeout error on SADD (async) for {set_key}: {e}. Will attempt reconnect.")
             await self.close_connection()
             return None
        except Exception as e:
            logger.error(f"Unexpected error on SADD (async) for {set_key}: {e}", exc_info=True)
            return None

    async def smembers(self, set_key: str) -> set[str] | None:
        """Gets all members of a set ASYNCHRONOUSLY."""
        if not await self._ensure_connection() or not self.redis_client: return None
        try:
            members = await self.redis_client.smembers(set_key)
            logger.debug(f"SMEMBERS (async) on {set_key}: Found={len(members)}")
            return members
        # --- CORRECCIÓN: Usar excepciones importadas ---
        except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
             logger.error(f"Connection/Timeout error on SMEMBERS (async) for {set_key}: {e}. Will attempt reconnect.")
             await self.close_connection()
             return None
        except Exception as e:
            logger.error(f"Unexpected error on SMEMBERS (async) for {set_key}: {e}", exc_info=True)
            return None

    async def ping(self) -> bool:
         """Performs a PING command ASYNCHRONOUSLY. Returns True if successful, False otherwise."""
         if not self.redis_client:
             logger.warning("Ping check failed: Redis client is None.")
             return False
         try:
             result = await asyncio.wait_for(self.redis_client.ping(), timeout=self._socket_timeout)
             return result
         # --- CORRECCIÓN: Usar excepciones importadas ---
         except (RedisConnectionError, RedisTimeoutError, asyncio.TimeoutError) as e:
             logger.warning(f"Ping failed during health check (async): {e}")
             await self.close_connection()
             return False
         except Exception as e:
             logger.error(f"Unexpected error during ping (async): {e}", exc_info=True)
             await self.close_connection()
             return False

    async def close_connection(self):
        """Closes the Redis connection if it's open. ASYNCHRONOUS."""
        client = self.redis_client
        if client:
            self.redis_client = None
            try:
                await client.close()
                # await client.connection_pool.disconnect()
                logger.info("Async Redis connection closed.")
            except Exception as e:
                logger.error(f"Error closing async Redis connection: {e}", exc_info=True)


# --- Example Usage (necesita adaptarse para asyncio o eliminarse) ---
# El bloque if __name__ == "__main__" original necesitaría usar asyncio.run()
# y convertir las llamadas a los métodos del manager a `await`.
# Ejemplo de cómo se podría adaptar:
async def main_async_example():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        return # Salir si la config falla

    redis_manager = RedisManager(config)

    logger.info("Checking initial Redis connection (async example)...")
    if await redis_manager._ensure_connection():
        logger.info("Initial connection verified (async example).")

        logger.info("\n--- Testing SADD/SMEMBERS (async example) ---")
        await redis_manager.sadd("managed_topics_async_test", "topicA")
        await redis_manager.sadd("managed_topics_async_test", "topicB")
        topics = await redis_manager.smembers("managed_topics_async_test")
        print(f"Managed topics (async example): {topics}")

        # ... adaptar el resto de las pruebas con await ...

        # Simular heartbeat (necesitaría await si se usa redis_manager para 'set')
        consumer1_id = "consumer-async-1"
        if redis_manager.redis_client: # Aún necesitamos acceso directo para 'set' si no hay wrapper async
             try:
                 # Idealmente, tendríamos un wrapper async `set_with_ttl` en RedisManager
                 # Por ahora, usamos el cliente directamente (que es async)
                 await redis_manager.redis_client.set(
                     f"{config.heartbeat_key_prefix}{consumer1_id}",
                     "alive",
                     ex=config.heartbeat_ttl_seconds
                 )
                 logger.info(f"Simulated heartbeat for {consumer1_id} (async)")
             except Exception as e:
                 logger.error(f"Error simulating heartbeat for {consumer1_id} (async): {e}")


        logger.info("\n--- Testing Lua Script (async example) ---")
        assignments_key = "assignments:topicA:groupAsync"
        num_parts = config.num_partitions
        partition1 = await redis_manager.execute_assignment_script(assignments_key, consumer1_id, num_parts)
        print(f"Partition assigned to {consumer1_id}: {partition1}")

        # Cerrar conexión al final
        await redis_manager.close_connection()
    else:
        logger.error("Could not establish initial Redis connection (async example). Terminating.")


if __name__ == "__main__":
    # Ejecutar el ejemplo asíncrono
    # Comenta o elimina esto si no necesitas correr el ejemplo directamente
    print("Running RedisManager async example...")
    try:
        asyncio.run(main_async_example())
    except KeyboardInterrupt:
        print("Async example interrupted.")
    print("RedisManager async example finished.")