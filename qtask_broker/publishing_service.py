# qtask_broker/publishing_service.py
import json
import logging
import asyncio  # Importar asyncio (buena práctica)

# Importar dependencias necesarias (ahora RedisManager es async)
from qtask_broker.redis_manager import (
    RedisManager,
)  # Importa el RedisManager async # cite: 5
from qtask_broker.partition_manager import (
    PartitionManager,
)  # Sin cambios necesarios aquí # cite: 5

# Importar la constante desde SubscriptionService
from qtask_broker.subscription_service import SubscriptionService  # cite: 5

# Importar ConfigurationLoader para type hinting si es necesario (no usado directamente)
from qtask_broker.config import ConfigurationLoader  # cite: 5

import os
import time

logger = logging.getLogger(__name__)


class PublishingService:
    """
    Manages the publication of messages ASYNCHRONOUSLY to the correct partitions,
    ensuring serialization and registration of the base topic.
    Includes detailed timing logs for Redis operations.
    """

    MANAGED_TOPICS_KEY = SubscriptionService.MANAGED_TOPICS_KEY  # cite: 5

    def __init__(
        self, redis_manager: RedisManager, partition_manager: PartitionManager
    ):  # cite: 5
        """
        Initializes the PublishingService with an ASYNCHRONOUS RedisManager.

        Args:
            redis_manager: An ASYNCHRONOUS RedisManager instance.
            partition_manager: A PartitionManager instance (sigue siendo síncrono).
        """
        self.redis_manager = redis_manager  # cite: 5
        self.partition_manager = partition_manager  # cite: 5
        logger.info("PublishingService initialized (for async).")  # cite: 5

    async def publish_message(
        self, topic: str, partition_key: str, data: dict
    ) -> tuple[bool, int | None, str | None]:  # cite: 5
        """
        Publishes a message ASYNCHRONOUSLY to the appropriate partition of a topic.
        Includes detailed timing logs.

        Args:
            topic: The base topic name.
            partition_key: The key used to determine the partition.
            data: The Python dictionary containing the message data to publish.

        Returns:
            A tuple (success: bool, partition_index: int | None, message_id: str | None):
            - success: True if message published and topic registered, False otherwise.
            - partition_index: The index where it was published, or None if failed before.
            - message_id: The message ID returned by Redis (XADD), or None if failed.
        """
        logger.info(
            f"[PID:{os.getpid()}] Attempting to publish message (async) to topic '{topic}' with partition key '{partition_key}'."
        )  # cite: 5

        # 1. Determinar la partición destino (síncrono)
        start_calc_part = time.monotonic()
        try:
            target_partition = self.partition_manager.get_partition_index(
                partition_key
            )  # cite: 5
            # logger.debug(f"Key '{partition_key}' mapped to partition {target_partition}.") # cite: 5
        except Exception as e:
            logger.error(
                f"[PID:{os.getpid()}] Unexpected error calculating partition for key '{partition_key}': {e}",
                exc_info=True,
            )  # cite: 5
            return False, None, None  # cite: 5
        end_calc_part = time.monotonic()
        logger.debug(
            f"[PID:{os.getpid()}] Partition calculation took {end_calc_part - start_calc_part:.6f} seconds. Key='{partition_key}' -> Partition={target_partition}."
        )

        # 2. Obtener la clave del stream particionado (síncrono)
        start_get_key = time.monotonic()
        try:
            stream_key = self.partition_manager.get_partitioned_stream_key(
                topic, target_partition
            )  # cite: 5
            # logger.debug(f"Target stream key: '{stream_key}'.") # cite: 5
        except ValueError as e:
            logger.error(
                f"[PID:{os.getpid()}] Error generating stream key for partition {target_partition}: {e}"
            )  # cite: 5
            return False, target_partition, None  # cite: 5
        end_get_key = time.monotonic()
        logger.debug(
            f"[PID:{os.getpid()}] Stream key generation took {end_get_key - start_get_key:.6f} seconds. Key='{stream_key}'."
        )

        # 3. Serializar los datos a JSON (síncrono)
        start_json = time.monotonic()
        try:
            message_payload = {"payload": json.dumps(data)}  # cite: 5
        except (TypeError, OverflowError) as e:
            logger.error(
                f"[PID:{os.getpid()}] Error serializing data to JSON for topic '{topic}': {e}",
                exc_info=True,
            )  # cite: 5
            return False, target_partition, None  # cite: 5
        end_json = time.monotonic()
        logger.debug(
            f"[PID:{os.getpid()}] JSON serialization took {end_json - start_json:.6f} seconds."
        )

        # --- 4. Publicar el mensaje usando RedisManager (ASÍNCRONO - XADD) ---
        message_id = None
        logger.info(
            f"[PID:{os.getpid()}] STREAM_KEY: {stream_key}. Preparing to call XADD."
        )  # Log ANTES de XADD
        start_time_xadd = time.monotonic()
        try:
            message_id = await self.redis_manager.xadd(
                stream_key, message_payload
            )  # cite: 5
        finally:
            # Este bloque finally se ejecuta incluso si await xadd lanza una excepción
            end_time_xadd = time.monotonic()
            duration_xadd = end_time_xadd - start_time_xadd
            logger.info(
                f"[PID:{os.getpid()}] XADD for {stream_key} finished. Duration: {duration_xadd:.6f} seconds. Result: {message_id}"
            )  # Log DESPUÉS de XADD
            # Loguear si tarda demasiado (más que el timeout del cliente - 1s de margen)
            if duration_xadd > 9.0:
                logger.warning(
                    f"[PID:{os.getpid()}] SLOW XADD DETECTED! Took {duration_xadd:.6f}s for stream {stream_key}, client likely timed out."
                )

        # Verificar resultado después del finally
        if message_id is None:
            # RedisManager.xadd ya loggeó el error específico (conexión, etc.)
            logger.error(
                f"[PID:{os.getpid()}] Failed to publish message (XADD async resulted in None) to stream '{stream_key}'."
            )  # cite: 5
            return False, target_partition, None  # cite: 5

        logger.info(
            f"[PID:{os.getpid()}] Message published successfully (async) to '{stream_key}' with ID: {message_id}."
        )  # cite: 5

        # --- 5. Registrar el topic base (ASÍNCRONO - SADD) ---
        sadd_result = None
        logger.info(
            f"[PID:{os.getpid()}] TOPIC: {topic}. Preparing to call SADD for key '{self.MANAGED_TOPICS_KEY}'."
        )  # Log ANTES de SADD
        start_time_sadd = time.monotonic()
        try:
            sadd_result = await self.redis_manager.sadd(
                self.MANAGED_TOPICS_KEY, topic
            )  # cite: 5
        finally:
            # Este bloque finally se ejecuta incluso si await sadd lanza una excepción
            end_time_sadd = time.monotonic()
            duration_sadd = end_time_sadd - start_time_sadd
            logger.info(
                f"[PID:{os.getpid()}] SADD for {topic} finished. Duration: {duration_sadd:.6f} seconds. Result: {sadd_result}"
            )  # Log DESPUÉS de SADD
            # Loguear si tarda demasiado
            if duration_sadd > 1.0:  # SADD debería ser rápido, 1 segundo ya es mucho
                logger.warning(
                    f"[PID:{os.getpid()}] SLOW SADD DETECTED! Took {duration_sadd:.6f}s for topic {topic}."
                )

        # Verificar resultado después del finally
        if sadd_result is None:
            # RedisManager.sadd ya loggeó el error específico (conexión, etc.)
            logger.warning(
                f"[PID:{os.getpid()}] Failed to register base topic '{topic}' (async) (SADD resulted in None) in set '{self.MANAGED_TOPICS_KEY}' (message WAS published)."
            )  # cite: 5
            # Continuamos considerando la publicación exitosa
            return True, target_partition, message_id  # cite: 5
        elif sadd_result == 1:
            logger.info(
                f"[PID:{os.getpid()}] Base topic '{topic}' added to set '{self.MANAGED_TOPICS_KEY}' (async)."
            )  # cite: 5
        # else: sadd_result == 0 (ya existía)

        # Si llegamos aquí, todo fue exitoso
        return True, target_partition, message_id  # cite: 5


# --- Example Usage (necesita adaptarse para asyncio o eliminarse) ---
# El bloque if __name__ == "__main__" original necesitaría usar asyncio.run()
# y adaptar las llamadas a publish_message con `await`, además de asegurar
# que redis_manager se inicialice y conecte de forma asíncrona.


async def main_async_example():
    # Configurar logging básico para el ejemplo
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    )

    # Necesitamos estas clases para el ejemplo
    from qtask_broker.config import ConfigurationLoader  # cite: 5
    from qtask_broker.redis_manager import RedisManager  # cite: 5
    from qtask_broker.partition_manager import PartitionManager  # cite: 5
    from qtask_broker.subscription_service import (
        SubscriptionService,
    )  # Solo para la constante # cite: 5

    # 1. Cargar configuración
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        return

    # 2. Crear dependencias (RedisManager async)
    try:
        redis_manager = RedisManager(config)
        partition_manager = PartitionManager(config)  # PartitionManager no cambia
    except ValueError as e:  # Capturar errores de init de PartitionManager
        logger.error(f"Error initializing managers: {e}")
        return

    # 3. Crear PublishingService
    publishing_service = PublishingService(redis_manager, partition_manager)

    # 4. Probar publicación (requiere conexión Redis async)
    logger.info("\n--- Testing PublishingService (async example) ---")
    topic = "user_activity_async"  # cite: 5
    key1 = "user:1001async"  # cite: 5
    data1 = {"action": "login", "timestamp": "2025-04-29T18:00:00Z"}  # cite: 5

    key2 = "user:2050async"  # cite: 5
    data2 = {"action": "view_page", "url": "/products/456"}  # cite: 5

    data_invalid_json = {"set_object": {1, 2, 3}}  # No serializable # cite: 5

    # Asegurar conexión inicial async
    if not await redis_manager._ensure_connection():
        logger.error("Could not connect to Redis for the async example.")
        return

    print(f"\nPublishing message 1 (key: {key1}) (async)...")
    # Usar await
    success1, part1, id1 = await publishing_service.publish_message(
        topic, key1, data1
    )  # cite: 5
    print(f"Result 1: Success={success1}, Partition={part1}, ID={id1}")  # cite: 5

    print(f"\nPublishing message 2 (key: {key2}) (async)...")
    # Usar await
    success2, part2, id2 = await publishing_service.publish_message(
        topic, key2, data2
    )  # cite: 5
    print(f"Result 2: Success={success2}, Partition={part2}, ID={id2}")  # cite: 5
    if part1 is not None and part2 is not None:
        print(
            f"(Note: Partition {part1} for {key1} and {part2} for {key2} may be the same or different)"
        )  # cite: 5

    print(f"\nAttempting to publish non-serializable data (async)...")
    # Usar await
    success_err, part_err, id_err = await publishing_service.publish_message(
        topic, key1, data_invalid_json
    )  # cite: 5
    print(
        f"JSON Error Result (should be False): Success={success_err}, Partition={part_err}, ID={id_err}"
    )  # cite: 5

    # Verificar el set de topics gestionados (opcional)
    if redis_manager.redis_client:
        # Usar await para smembers
        managed_topics = await redis_manager.smembers(
            PublishingService.MANAGED_TOPICS_KEY
        )  # cite: 5
        print(
            f"\nManaged topics in Redis (async): {managed_topics}"
        )  # Debería incluir 'user_activity_async'

    # Cerrar conexión al final
    await redis_manager.close_connection()


if __name__ == "__main__":
    # Ejecutar el ejemplo asíncrono
    # Comenta o elimina esto si no necesitas correr el ejemplo directamente
    print("Running PublishingService async example...")
    try:
        asyncio.run(main_async_example())
    except KeyboardInterrupt:
        print("Async example interrupted.")
    print("PublishingService async example finished.")
