# qtask_broker/subscription_service.py
import logging
import asyncio # Importar asyncio (buena práctica)

# Importar dependencias necesarias (RedisManager ahora es async)
from qtask_broker.redis_manager import RedisManager # Importar RedisManager async # cite: 7
from qtask_broker.partition_manager import PartitionManager # PartitionManager no cambia # cite: 7
# Importar ConfigurationLoader para type hinting (no usado directamente)
from qtask_broker.config import ConfigurationLoader # cite: 7

logger = logging.getLogger(__name__)

class SubscriptionService:
    """
    Manages group subscriptions to topic partitions ASYNCHRONOUSLY, ensuring
    the existence of the stream and group in Redis, and registering the base topic.
    """
    MANAGED_TOPICS_KEY = 'managed_topics' # Constante sin cambios # cite: 7

    def __init__(self, redis_manager: RedisManager, partition_manager: PartitionManager): # cite: 7
        """
        Initializes the SubscriptionService with an ASYNCHRONOUS RedisManager.

        Args:
            redis_manager: An ASYNCHRONOUS RedisManager instance.
            partition_manager: A PartitionManager instance (sigue siendo síncrono).
        """
        self.redis_manager = redis_manager # cite: 7
        self.partition_manager = partition_manager # cite: 7
        logger.info("SubscriptionService initialized (for async).") # cite: 7

    # CAMBIO: Convertido a async def
    async def ensure_subscription(self, topic: str, group: str, partition_index: int) -> bool: # cite: 7
        """
        Ensures ASYNCHRONOUSLY that a group is subscribed to a specific partition.

        This involves:
        1. Getting the partitioned stream key.
        2. Attempting to create the group for that stream (using MKSTREAM).
        3. Adding the base topic to the set of managed topics.

        Handles 'BUSYGROUP' as success. Returns True if successful, False on error.

        Args:
            topic: The base topic name.
            group: The consumer group name.
            partition_index: The numerical index of the partition.

        Returns:
            True if subscription ensured, False otherwise.
        """
        logger.info(f"Ensuring subscription (async) for group '{group}' on partition {partition_index} of topic '{topic}'.") # cite: 7

        # 1. Obtener la clave del stream particionado (síncrono)
        try:
            stream_key = self.partition_manager.get_partitioned_stream_key(topic, partition_index) # cite: 7
            logger.debug(f"Partitioned stream key obtained: '{stream_key}'") # cite: 7
        except ValueError as e:
            # El índice de partición era inválido según PartitionManager
            logger.error(f"Cannot ensure subscription (async): {e}") # cite: 7
            return False # cite: 7

        # 2. Intentar crear el grupo y el stream si no existen (ASÍNCRONO - XGROUP CREATE)
        # RedisManager.xgroup_create async maneja BUSYGROUP como éxito (devuelve True)
        # y también maneja errores de conexión.
        # Usar await para la llamada asíncrona
        group_created_or_exists = await self.redis_manager.xgroup_create( # cite: 7
            stream_key=stream_key,
            group_name=group,
            mkstream=True # Importante: crea el stream si no existe # cite: 7
        )

        if not group_created_or_exists:
            # Ocurrió un error diferente a BUSYGROUP o un error de conexión irrecuperable.
            logger.error(f"Critical failure attempting to create/verify group '{group}' on stream '{stream_key}' (async).") # cite: 7
            return False # No se pudo asegurar el grupo # cite: 7

        # 3. Añadir el topic base al set de topics gestionados (ASÍNCRONO - SADD)
        # Usamos SADD, que es idempotente.
        # Usar await para la llamada asíncrona
        sadd_result = await self.redis_manager.sadd(self.MANAGED_TOPICS_KEY, topic) # cite: 7

        if sadd_result is None:
            # Ocurrió un error de conexión o similar durante SADD.
            logger.error(f"Critical failure attempting to add base topic '{topic}' to set '{self.MANAGED_TOPICS_KEY}' (async).") # cite: 7
            return False # La suscripción no está completamente asegurada # cite: 7

        if sadd_result == 1:
            logger.info(f"Base topic '{topic}' added to set '{self.MANAGED_TOPICS_KEY}' (async).") # cite: 7
        else: # sadd_result == 0
            logger.info(f"Base topic '{topic}' already existed in set '{self.MANAGED_TOPICS_KEY}' (async).") # cite: 7

        # Si llegamos aquí, tanto el grupo/stream como el registro del topic están asegurados
        logger.info(f"Subscription for group '{group}' on partition {partition_index} of topic '{topic}' ensured successfully (async).") # cite: 7
        return True # cite: 7

# --- Example Usage (necesita adaptarse para asyncio o eliminarse) ---
# El bloque if __name__ == "__main__" original necesitaría usar asyncio.run()
# y adaptar las llamadas a ensure_subscription con `await`, además de asegurar
# que redis_manager se inicialice y conecte de forma asíncrona.

async def main_async_example():
    # Configurar logging básico para el ejemplo
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    # Necesitamos estas clases para el ejemplo
    from qtask_broker.config import ConfigurationLoader # cite: 7
    from qtask_broker.redis_manager import RedisManager # cite: 7
    from qtask_broker.partition_manager import PartitionManager # cite: 7

    # 1. Cargar configuración
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        return

    # 2. Crear dependencias (RedisManager async)
    try:
        redis_manager = RedisManager(config)
        partition_manager = PartitionManager(config) # PartitionManager no cambia
    except ValueError as e: # Capturar errores de init de PartitionManager
         logger.error(f"Error initializing managers: {e}")
         return

    # 3. Crear SubscriptionService
    subscription_service = SubscriptionService(redis_manager, partition_manager)

    # 4. Probar asegurar suscripciones (requiere conexión Redis async)
    logger.info("\n--- Testing SubscriptionService (async example) ---")
    topic1 = "orders_async" # cite: 7
    group1 = "order_processors_async" # cite: 7
    partition_idx1 = 0 # cite: 7
    partition_idx2 = 1 # cite: 7

    topic2 = "inventory_updates_async" # cite: 7
    group2 = "stock_alerters_async" # cite: 7
    partition_idx3 = 0 # cite: 7

    # Asegurar conexión inicial async
    if not await redis_manager._ensure_connection():
        logger.error("Could not connect to Redis for the async example.")
        return

    print(f"\nEnsuring subscription for {topic1}/{group1} on partition {partition_idx1} (async)...")
    # Usar await
    success1 = await subscription_service.ensure_subscription(topic1, group1, partition_idx1) # cite: 7
    print(f"Result 1: {success1}") # cite: 7

    print(f"\nEnsuring subscription again for {topic1}/{group1} on partition {partition_idx1} (async)...")
    # Usar await
    success2 = await subscription_service.ensure_subscription(topic1, group1, partition_idx1) # cite: 7
    print(f"Result 2 (should be True): {success2}") # cite: 7

    print(f"\nEnsuring subscription for {topic1}/{group1} on partition {partition_idx2} (async)...")
    # Usar await
    success3 = await subscription_service.ensure_subscription(topic1, group1, partition_idx2) # cite: 7
    print(f"Result 3: {success3}") # cite: 7

    print(f"\nEnsuring subscription for {topic2}/{group2} on partition {partition_idx3} (async)...")
    # Usar await
    success4 = await subscription_service.ensure_subscription(topic2, group2, partition_idx3) # cite: 7
    print(f"Result 4: {success4}") # cite: 7

    print(f"\nAttempting to ensure subscription with invalid index (async)...")
    # Usar await
    success_invalid = await subscription_service.ensure_subscription(topic1, group1, config.num_partitions) # Índice fuera de rango # cite: 7
    print(f"Invalid index result (should be False): {success_invalid}") # cite: 7


    # Verificar el set de topics gestionados (opcional)
    if redis_manager.redis_client:
        # Usar await para smembers
        managed_topics = await redis_manager.smembers(SubscriptionService.MANAGED_TOPICS_KEY) # cite: 7
        print(f"\nManaged topics in Redis (async): {managed_topics}") # Debería incluir 'orders_async' e 'inventory_updates_async'

    # Cerrar conexión al final
    await redis_manager.close_connection()


if __name__ == "__main__":
    # Ejecutar el ejemplo asíncrono
    # Comenta o elimina esto si no necesitas correr el ejemplo directamente
    print("Running SubscriptionService async example...")
    try:
        asyncio.run(main_async_example())
    except KeyboardInterrupt:
        print("Async example interrupted.")
    print("SubscriptionService async example finished.")