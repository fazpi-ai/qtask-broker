# qtask_broker/topic_service.py
import logging
import asyncio  # Importar asyncio (buena práctica)
from typing import List, Set, Optional  # Sin cambios en imports # cite: 8

# Importar dependencias necesarias (RedisManager ahora es async)
from qtask_broker.redis_manager import (
    RedisManager,
)  # Importar RedisManager async # cite: 8

# Importar la constante desde SubscriptionService
from qtask_broker.subscription_service import SubscriptionService  # cite: 8

# Importar ConfigurationLoader para type hinting (no usado directamente)
from qtask_broker.config import ConfigurationLoader  # cite: 8


logger = logging.getLogger(__name__)


class TopicService:
    """
    Provides functionality to retrieve the list of managed base topics
    from Redis ASYNCHRONOUSLY.
    """

    MANAGED_TOPICS_KEY = (
        SubscriptionService.MANAGED_TOPICS_KEY
    )  # Constante sin cambios # cite: 8

    def __init__(self, redis_manager: RedisManager):  # cite: 8
        """
        Initializes the TopicService with an ASYNCHRONOUS RedisManager.

        Args:
            redis_manager: An ASYNCHRONOUS RedisManager instance.
        """
        self.redis_manager = redis_manager  # cite: 8
        logger.info("TopicService initialized (for async).")  # cite: 8

    # CAMBIO: Convertido a async def
    async def get_managed_topics(self) -> List[str]:  # cite: 8
        """
        Gets the list of all base topics ASYNCHRONOUSLY from the 'managed_topics'
        set in Redis.

        Returns:
            A list of strings with the base topic names, sorted alphabetically.
            Returns an empty list if there are no topics or if an error occurs
            communicating with Redis.
        """
        logger.info(
            f"Getting list of managed topics (async) from key '{self.MANAGED_TOPICS_KEY}'..."
        )  # cite: 8

        # Llamar a smembers (async) via RedisManager
        # Usar await para la llamada asíncrona
        # redis_manager.smembers devuelve un set o None en caso de error
        topics_set: Optional[Set[str]] = await self.redis_manager.smembers(
            self.MANAGED_TOPICS_KEY
        )  # cite: 8

        if topics_set is None:
            # Ocurrió un error (ej. conexión), RedisManager ya lo loggeó.
            logger.error(
                f"Could not retrieve topic list from Redis (async) (key: {self.MANAGED_TOPICS_KEY}). Returning empty list."
            )  # cite: 8
            return []  # Devolver lista vacía en error # cite: 8

        # Convertir el set a una lista ordenada para una respuesta consistente
        topics_list = sorted(list(topics_set))  # cite: 8
        logger.info(f"Found {len(topics_list)} managed topics (async).")  # cite: 8
        # logger.debug(f"Topics found (async): {topics_list}") # Opcional: log detallado

        return topics_list  # cite: 8


# --- Example Usage (necesita adaptarse para asyncio o eliminarse) ---
# El bloque if __name__ == "__main__" original necesitaría usar asyncio.run()
# y adaptar las llamadas a get_managed_topics con `await`, además de asegurar
# que redis_manager se inicialice y conecte de forma asíncrona.


async def main_async_example():
    # Configurar logging básico para el ejemplo
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(module)s - %(message)s",
    )

    # Importar clases necesarias para el ejemplo
    from qtask_broker.config import ConfigurationLoader  # cite: 8
    from qtask_broker.redis_manager import RedisManager  # cite: 8

    # Necesitamos SubscriptionService solo para la constante en este contexto
    from qtask_broker.subscription_service import SubscriptionService  # cite: 8

    # 1. Cargar configuración
    try:
        config = ConfigurationLoader()
    except ValueError as e:
        logger.error(f"Error loading configuration: {e}")
        return

    # 2. Crear RedisManager (instancia async)
    try:
        redis_manager = RedisManager(config)
    except ValueError as e:
        logger.error(f"Error initializing RedisManager: {e}")
        return

    # 3. Crear TopicService
    topic_service = TopicService(redis_manager)

    # 4. Obtener topics (requiere conexión Redis async)
    logger.info("\n--- Testing TopicService (async example) ---")

    # Asegurar conexión inicial async
    if not await redis_manager._ensure_connection():
        logger.error("Could not connect to Redis for the async example.")
        return

    # (Opcional) Añadir topics de ejemplo si Redis está vacío (usar await)
    print("\n(Ensuring some example topics exist for async test...)")  # cite: 8
    await redis_manager.sadd(
        TopicService.MANAGED_TOPICS_KEY, "example_topic_A"
    )  # cite: 8
    await redis_manager.sadd(
        TopicService.MANAGED_TOPICS_KEY, "example_topic_C"
    )  # cite: 8
    await redis_manager.sadd(
        TopicService.MANAGED_TOPICS_KEY, "example_topic_B"
    )  # cite: 8

    # Obtener la lista (usar await)
    print("\nGetting list of topics (async)...")  # cite: 8
    topic_list = await topic_service.get_managed_topics()  # cite: 8

    if topic_list:  # cite: 8
        print("\nManaged topics found (async):")  # cite: 8
        for topic in topic_list:  # cite: 8
            print(f"- {topic}")  # cite: 8
    else:  # cite: 8
        print("\nNo managed topics found or an error occurred (async).")  # cite: 8

    # Cerrar conexión al final (usar await)
    await redis_manager.close_connection()


if __name__ == "__main__":
    # Ejecutar el ejemplo asíncrono
    # Comenta o elimina esto si no necesitas correr el ejemplo directamente
    print("Running TopicService async example...")
    try:
        asyncio.run(main_async_example())
    except KeyboardInterrupt:
        print("Async example interrupted.")
    print("TopicService async example finished.")
