# qtask_broker/assignment_service.py
import logging
import asyncio # Importar asyncio si necesitas usar alguna de sus funciones (no directamente aquí, pero buena práctica)

# Importar el RedisManager refactorizado y ConfigurationLoader
from qtask_broker.redis_manager import RedisManager # Importa el nuevo RedisManager async # cite: 1
from qtask_broker.config import ConfigurationLoader # Needed indirectly via RedisManager # cite: 1

logger = logging.getLogger(__name__)

class AssignmentService:
    """
    Encapsulates the logic for assigning partitions to consumers ASYNCHRONOUSLY
    by executing the Lua script via RedisManager.
    """
    def __init__(self, redis_manager: RedisManager): # cite: 1
        """
        Initializes the AssignmentService with an ASYNCHRONOUS RedisManager.

        Args:
            redis_manager: An ASYNCHRONOUS RedisManager instance.
        """
        # La inicialización no cambia, solo el tipo de redis_manager que recibe
        self.redis_manager = redis_manager # cite: 1
        self.config: ConfigurationLoader = redis_manager.config # cite: 1
        logger.info("AssignmentService initialized (for async).") # cite: 1

    def _get_assignments_key(self, base_topic: str, group: str) -> str: # cite: 1
        """
        Generates the hash key used to store partition -> consumer assignments.
        (No cambia, es lógica síncrona pura).

        Args:
            base_topic: The base topic name.
            group: The consumer group name.

        Returns:
            The assignments key (e.g., 'assignments:my_topic:my_group').
        """
        key = f"assignments:{base_topic}:{group}" # cite: 1
        logger.debug(f"Generated assignments key: '{key}'") # cite: 1
        return key # cite: 1

    # CAMBIO: Convertido a async def
    async def assign_partition(self, topic: str, group: str, consumer_id: str) -> int: # cite: 1
        """
        Attempts to assign an available partition of the specified topic/group
        to the given consumer ASYNCHRONOUSLY by executing the Lua script.

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
        logger.info(f"Async assignment request received for consumer '{consumer_id}' on topic '{topic}', group '{group}'.") # cite: 1

        # 1. Get the key where assignments are stored (síncrono)
        assignments_key = self._get_assignments_key(topic, group) # cite: 1

        # 2. Get necessary parameters from configuration (síncrono)
        num_partitions = self.config.num_partitions # cite: 1

        # Validate that num_partitions is valid (síncrono)
        if num_partitions <= 0: # cite: 1
             logger.error(f"Invalid configuration detected in AssignmentService (async): num_partitions={num_partitions}") # cite: 1
             return -1 # Cannot assign without valid partitions # cite: 1

        # 3. Execute the Lua script via RedisManager (ASÍNCRONO)
        logger.debug(f"Calling redis_manager.execute_assignment_script (async) with key='{assignments_key}', consumer='{consumer_id}', partitions={num_partitions}") # cite: 1
        # Usar await para llamar al método async del manager refactorizado
        assigned_partition = await self.redis_manager.execute_assignment_script( # cite: 1
            assignments_key=assignments_key,
            consumer_id=consumer_id,
            num_partitions=num_partitions
            # heartbeat_prefix se usa internamente en redis_manager
        )

        # 4. Interpret the result (síncrono)
        if assigned_partition >= 0: # cite: 1
            logger.info(f"Lua script (async) successfully assigned partition {assigned_partition} to '{consumer_id}' for '{topic}/{group}'.") # cite: 1
            return assigned_partition # cite: 1
        else:
            # El valor -1 puede significar "sin particiones libres" o un error interno.
            # RedisManager ya debería haber loggeado errores específicos (conexión, script).
            logger.warning(f"Could not assign partition (async) for '{consumer_id}' on '{topic}/{group}'. Script returned {assigned_partition}.") # cite: 1
            return -1 # cite: 1

# --- Example Usage (necesita adaptarse para asyncio o eliminarse) ---
# El bloque if __name__ == "__main__" original necesitaría usar asyncio.run()
# y adaptar las llamadas a assign_partition con `await`, además de asegurar
# que redis_manager se inicialice y conecte de forma asíncrona.

async def main_async_example():
    # Configurar logging básico para el ejemplo
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # 1. Cargar configuración
    try:
        config = ConfigurationLoader() # cite: 1
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        return

    # 2. Crear RedisManager (instancia, pero no conecta aquí)
    redis_manager = RedisManager(config) # cite: 1

    # 3. Crear AssignmentService
    assignment_service = AssignmentService(redis_manager) # cite: 1

    # 4. Simular asignaciones (requiere conexión Redis async)
    logger.info("\n--- Testing AssignmentService (async example) ---")
    topic = "user_events_async" # cite: 1
    group = "processor_group_async" # cite: 1
    consumer_a = "worker-alpha-async" # cite: 1
    consumer_b = "worker-beta-async" # cite: 1

    # Asegurar conexión inicial (MUY IMPORTANTE en async)
    if not await redis_manager._ensure_connection(): # Usa await
        logger.error("Could not connect to Redis for the async example.")
        return

    # Simular heartbeats (necesario para que Lua los considere vivos)
    try:
        # Usar el cliente async directamente (o crear wrappers async en RedisManager)
        if redis_manager.redis_client:
            # Usar await para las operaciones 'set' asíncronas
            await redis_manager.redis_client.set(f"{config.heartbeat_key_prefix}{consumer_a}", "alive", ex=config.heartbeat_ttl_seconds) # cite: 1
            await redis_manager.redis_client.set(f"{config.heartbeat_key_prefix}{consumer_b}", "alive", ex=config.heartbeat_ttl_seconds) # cite: 1
            logger.info(f"Simulated heartbeats for {consumer_a} and {consumer_b} (async).") # cite: 1
        else:
             logger.warning("Cannot simulate heartbeats, async Redis client not connected.") # cite: 1
    except Exception as e:
        logger.warning(f"Could not simulate heartbeats (Redis unavailable? Async error?): {e}") # cite: 1


    print(f"\nAttempting assignment for {consumer_a} (async)...")
    # Usar await para la llamada asíncrona
    partition_a = await assignment_service.assign_partition(topic, group, consumer_a) # cite: 1
    print(f"Result for {consumer_a}: {partition_a}") # cite: 1

    print(f"\nAttempting assignment for {consumer_b} (async)...")
    # Usar await
    partition_b = await assignment_service.assign_partition(topic, group, consumer_b) # cite: 1
    print(f"Result for {consumer_b}: {partition_b}") # cite: 1

    print(f"\nAttempting assignment again for {consumer_a} (async)...")
    # Usar await
    partition_a_again = await assignment_service.assign_partition(topic, group, consumer_a) # cite: 1
    print(f"Re-assignment result for {consumer_a}: {partition_a_again} (should be {partition_a})") # cite: 1

    # Cerrar conexión Redis al final
    await redis_manager.close_connection() # Usa await


if __name__ == "__main__":
    # Ejecutar el ejemplo asíncrono
    # Comenta o elimina esto si no necesitas correr el ejemplo directamente
    print("Running AssignmentService async example...")
    try:
        asyncio.run(main_async_example())
    except KeyboardInterrupt:
        print("Async example interrupted.")
    print("AssignmentService async example finished.")