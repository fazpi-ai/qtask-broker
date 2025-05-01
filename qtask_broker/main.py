# qtask_broker/main.py
import logging
import asyncio # Importar asyncio
import json # Para manejo de errores de JSON en /push
from contextlib import asynccontextmanager
from typing import List, Dict, Any

# Web framework y validación de datos
from fastapi import FastAPI, HTTPException, Body, status, Response # Añadir Response para /health si se necesita status dinámico
from pydantic import BaseModel, Field
from datetime import datetime

# Importar clases (ahora los servicios y RedisManager son async)
from qtask_broker.config import ConfigurationLoader # cite: 3
from qtask_broker.redis_manager import RedisManager # Importa el RedisManager refactorizado # cite: 3
from qtask_broker.partition_manager import PartitionManager # PartitionManager sigue síncrono # cite: 3
from qtask_broker.assignment_service import AssignmentService # Importa el AssignmentService refactorizado # cite: 3
from qtask_broker.subscription_service import SubscriptionService # Importa el SubscriptionService refactorizado # cite: 3
from qtask_broker.publishing_service import PublishingService # Importa el PublishingService refactorizado # cite: 3
from qtask_broker.topic_service import TopicService # Importa el TopicService refactorizado # cite: 3

# --- Logging Configuration (sin cambios) ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
) # cite: 3
logger = logging.getLogger(__name__) # cite: 3

# --- Global instances (declaración) ---
# Se inicializarán dentro del lifespan para manejar el contexto async
config: ConfigurationLoader
redis_manager: RedisManager
partition_manager: PartitionManager
assignment_service: AssignmentService
subscription_service: SubscriptionService
publishing_service: PublishingService
topic_service: TopicService

# --- Application Lifespan (Modificado para async init/shutdown) ---
@asynccontextmanager
async def lifespan(app: FastAPI): # cite: 3
    """Handles application startup and shutdown events asynchronously."""
    # Hacer las variables globales para que estén disponibles en los endpoints
    global config, redis_manager, partition_manager, assignment_service, subscription_service, publishing_service, topic_service

    logger.info("Application starting up...")
    is_initialized = False # Flag para saber si cerrar redis en caso de error
    try:
        logger.info("Loading global configuration...")
        config = ConfigurationLoader() # cite: 3

        logger.info("Initializing RedisManager (async)...")
        redis_manager = RedisManager(config) # cite: 3
        is_initialized = True # Marcamos que redis_manager existe

        logger.info("Ensuring initial Redis connection (async)...")
        # Usar await para conectar y cargar script al inicio
        # _ensure_connection ya incluye _load_lua_script si la conexión es exitosa
        if not await redis_manager._ensure_connection(): # cite: 3 (Concepto _ensure_connection)
             # Decide si la app debe fallar al iniciar si Redis no conecta
             raise RuntimeError("Fatal: Could not establish initial Redis connection during startup.")
        logger.info("Initial Redis connection successful.")

        logger.info("Initializing other services (with async RedisManager)...")
        # Los __init__ de los servicios no cambian, solo el tipo de redis_manager
        partition_manager = PartitionManager(config) # cite: 3
        assignment_service = AssignmentService(redis_manager) # cite: 3
        subscription_service = SubscriptionService(redis_manager, partition_manager) # cite: 3
        publishing_service = PublishingService(redis_manager, partition_manager) # cite: 3
        topic_service = TopicService(redis_manager) # cite: 3

        logger.info("All services initialized successfully (async).") # cite: 3

    except ValueError as e: # Error en ConfigLoader o PartitionManager
        logger.error(f"Fatal error during initialization due to invalid configuration: {e}", exc_info=True)
        # No necesitamos cerrar redis aquí porque el error fue antes o en su init síncrono
        raise SystemExit(f"Configuration error: {e}") from e
    except Exception as e: # Otros errores (ej. RuntimeError de Redis)
        logger.error(f"Fatal error during service initialization (async): {e}", exc_info=True)
        # Asegurarse de cerrar redis si se inicializó parcialmente
        if is_initialized and redis_manager:
            # Crear una tarea para cerrar en background si estamos fallando al iniciar
            logger.warning("Attempting to close Redis connection after initialization failure...")
            asyncio.create_task(redis_manager.close_connection())
        raise SystemExit(f"Initialization error: {e}") from e

    # --- La aplicación se ejecuta aquí ---
    yield
    # -----------------------------------

    # --- Código de apagado ---
    logger.info("Application shutting down. Closing Redis connection (async)...")
    # Usar await para cerrar la conexión async
    if is_initialized and redis_manager: # Asegurar que exista
         await redis_manager.close_connection() # cite: 3 (Concepto close_connection)
    logger.info("Async Redis connection closed.")


# --- Crear Aplicación FastAPI ---
app = FastAPI(
    title="QTask Broker API (Async)", # cite: 3
    description="Centralized API for managing partitioned queues with Redis Streams (QTask). ASYNCHRONOUS.", # cite: 3
    version="1.1.0", # Versión actualizada
    lifespan=lifespan # Usar el lifespan refactorizado
) # cite: 3

# --- Modelos de Datos Pydantic (sin cambios) ---
class HealthResponse(BaseModel): # cite: 3 (Concepto de HealthResponse)
    status: str
    redis_connected: bool

class AssignPartitionRequest(BaseModel): # cite: 3
    topic: str = Field(..., min_length=1, description="Base topic name.")
    group: str = Field(..., min_length=1, description="Consumer group name.")
    consumerId: str = Field(..., min_length=1, description="Unique consumer ID.")

class AssignPartitionResponse(BaseModel): # cite: 3
    success: bool
    partitionIndex: int

class SubscribeRequest(BaseModel): # cite: 3
    topic: str = Field(..., min_length=1)
    group: str = Field(..., min_length=1)
    partitionIndex: int = Field(..., ge=0, description="Index of the partition to subscribe to (must be >= 0).")

class SubscribeResponse(BaseModel): # cite: 3
    success: bool
    message: str

class PushRequest(BaseModel): # cite: 3
    topic: str = Field(..., min_length=1)
    partitionKey: str = Field(..., min_length=1, description="Key used to determine the partition.")
    data: Dict[str, Any] # Accepts any valid JSON dictionary

class PushResponse(BaseModel): # cite: 3
    success: bool
    topic: str
    partition: int
    messageId: str

class TopicsResponse(BaseModel): # cite: 3
    topics: List[str]


# --- API Endpoints (Refactorizados para usar await y nuevo /health) ---

@app.get(
    "/health",
    response_model=HealthResponse,
    # status_code=status.HTTP_200_OK, # Status code dinámico abajo
    summary="Check application and Redis health",
    tags=["Health"]
)
async def health_check(response: Response): # Inyectar Response para status dinámico
    """Checks if the Redis connection is active via async ping."""
    is_connected = False
    # Verificar que redis_manager se haya inicializado en lifespan
    if 'redis_manager' in globals() and redis_manager:
        try:
            # Usar el método ping async del manager refactorizado
            is_connected = await redis_manager.ping()
        except Exception:
            # Ping puede fallar por varias razones, lo consideramos no conectado
            logger.warning("Health check ping encountered an exception.", exc_info=False) # No loguear stacktrace completo en health
            is_connected = False
    else:
        logger.warning("Health check executed before RedisManager was initialized.")

    # Establecer status code basado en la conexión
    response.status_code = status.HTTP_200_OK if is_connected else status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(status="ok" if is_connected else "error", redis_connected=is_connected)


@app.get(
    "/", # Endpoint raíz original # cite: 3
    tags=["Health"]
    # Puedes quitar este endpoint si /health es suficiente
)
async def get_root(): # cite: 3
     # Devuelve algo simple o redirige a la documentación
     return {"message": "QTask Broker API (Async) is running. Use /docs for API details."}


@app.post(
    "/assign-partition",
    response_model=AssignPartitionResponse,
    status_code=status.HTTP_200_OK, # 200 o 409 basado en lógica
    summary="Assigns a partition to a consumer (async)",
    tags=["Assignments"]
) # cite: 3
async def assign_partition_endpoint(request: AssignPartitionRequest): # cite: 3
    """
    Atomically attempts to assign an available partition ASYNCHRONOUSLY.
    """
    logger.info(f"Received POST /assign-partition request (async): {request.model_dump()}") # cite: 3
    try:
        # Usar await para llamar al servicio refactorizado
        assigned_partition = await assignment_service.assign_partition( # cite: 3
            topic=request.topic,
            group=request.group,
            consumer_id=request.consumerId
        )
        # La lógica de respuesta no cambia
        if assigned_partition >= 0: # cite: 3
            return AssignPartitionResponse(success=True, partitionIndex=assigned_partition) # cite: 3
        else:
            # Si el script Lua devuelve -1 (sin particiones disponibles)
            logger.warning(f"No available partitions found (async) for {request.consumerId} in {request.topic}/{request.group}") # cite: 3
            raise HTTPException( # cite: 3
                status_code=status.HTTP_409_CONFLICT,
                detail=f"No available partitions found for topic '{request.topic}', group '{request.group}'."
            )
    except HTTPException as http_exc:
         # Re-lanzar excepciones HTTP (como la 409)
         raise http_exc
    except Exception as e:
         # Capturar otros errores (ej. fallo de conexión en RedisManager)
         logger.error(f"Error processing /assign-partition (async): {e}", exc_info=True)
         raise HTTPException(
             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
             detail="Internal server error during partition assignment."
         )


@app.post(
    "/subscribe",
    response_model=SubscribeResponse,
    status_code=status.HTTP_200_OK,
    summary="Ensures a group subscription to a partition (async)",
    tags=["Subscriptions"]
) # cite: 3
async def subscribe_endpoint(request: SubscribeRequest): # cite: 3
    """
    Ensures the partitioned stream and consumer group exist ASYNCHRONOUSLY.
    """
    logger.info(f"Received POST /subscribe request (async): {request.model_dump()}") # cite: 3

    # Validación de índice (síncrona)
    if request.partitionIndex >= config.num_partitions: # cite: 3
         logger.error(f"Invalid partitionIndex {request.partitionIndex} for num_partitions={config.num_partitions}") # cite: 3
         raise HTTPException( # cite: 3
             status_code=status.HTTP_400_BAD_REQUEST,
             detail=f"Invalid partitionIndex: {request.partitionIndex}. Must be less than {config.num_partitions}."
         )

    try:
        # Usar await para llamar al servicio refactorizado
        success = await subscription_service.ensure_subscription( # cite: 3
            topic=request.topic,
            group=request.group,
            partition_index=request.partitionIndex
        )
        # Lógica de respuesta igual
        if success: # cite: 3
            return SubscribeResponse( # cite: 3
                success=True,
                message=f"Subscription (async) for group '{request.group}' on partition {request.partitionIndex} of topic '{request.topic}' ensured."
            )
        else:
            # Si el servicio devuelve False (error interno ya loggeado)
            raise HTTPException( # cite: 3
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error processing subscription."
            )
    except HTTPException as http_exc:
        # Re-lanzar excepciones HTTP (como la 400)
        raise http_exc
    except Exception as e:
         # Capturar otros errores
         logger.error(f"Error processing /subscribe (async): {e}", exc_info=True)
         raise HTTPException(
             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
             detail="Internal server error processing subscription."
         )


@app.post(
    "/push",
    response_model=PushResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Publishes a message to a topic partition (async)",
    tags=["Publishing"]
) # cite: 3
async def push_endpoint(request: PushRequest): # cite: 3
    """
    Publishes a message ASYNCHRONOUSLY after calculating partition.
    """
    # Loguear sin data completa por defecto
    logger.info(f"Received POST /push request (async): topic={request.topic}, partitionKey={request.partitionKey}, data_keys={list(request.data.keys())}") # cite: 3

    try:
        # Usar await para llamar al servicio refactorizado
        success, partition_index, message_id = await publishing_service.publish_message( # cite: 3
            topic=request.topic,
            partition_key=request.partitionKey,
            data=request.data
        )
        # Lógica de respuesta igual
        if success and partition_index is not None and message_id is not None: # cite: 3
            return PushResponse( # cite: 3
                success=True,
                topic=request.topic,
                partition=partition_index,
                messageId=message_id
            )
        else:
             # Si el servicio devuelve False o Nones (error interno ya loggeado)
             # Podríamos tener errores específicos si el servicio los devolviera
             raise HTTPException( # cite: 3
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Internal server error processing push request (async)."
             )
    except json.JSONDecodeError as e: # Error de serialización detectado en el servicio
         logger.error(f"JSON Serialization error processing /push (async): {e}", exc_info=True)
         raise HTTPException(
             status_code=status.HTTP_400_BAD_REQUEST,
             detail=f"Invalid JSON data provided: {e}"
         )
    except Exception as e:
         # Capturar otros errores
         logger.error(f"Error processing /push (async): {e}", exc_info=True)
         raise HTTPException(
             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
             detail="Internal server error processing push request."
         )


@app.get(
    "/topics",
    response_model=TopicsResponse,
    status_code=status.HTTP_200_OK,
    summary="Gets the list of managed base topics (async)",
    tags=["Topics"]
) # cite: 3
async def get_topics_endpoint(): # cite: 3
    """
    Returns a list of all base topic names ASYNCHRONOUSLY.
    """
    logger.info("Received GET /topics request (async)") # cite: 3
    try:
        # Usar await para llamar al servicio refactorizado
        topics_list = await topic_service.get_managed_topics() # cite: 3
        # El servicio devuelve [] en caso de error, lo cual está bien para la API
        return TopicsResponse(topics=topics_list) # cite: 3
    except Exception as e:
        # Capturar otros errores
        logger.error(f"Error processing /topics (async): {e}", exc_info=True)
        raise HTTPException(
             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
             detail="Internal server error retrieving topics."
         )


# --- Entry point para Uvicorn (sin cambios funcionales) ---
# Uvicorn se encarga de manejar el loop asyncio
if __name__ == "__main__": # cite: 3
    import uvicorn
    # Necesitamos cargar config aquí solo para obtener el puerto
    # si se corre directamente con 'python main.py'
    # Idealmente, el puerto se pasaría como argumento a uvicorn
    temp_config_for_run = ConfigurationLoader()
    run_port = temp_config_for_run.port
    logger.info(f"Starting Uvicorn server on http://0.0.0.0:{run_port} (async)")
    # host="0.0.0.0" para escuchar en todas las interfaces
    uvicorn.run("main:app", host="0.0.0.0", port=run_port, reload=False) # reload=True para desarrollo
    # Ejemplo: uvicorn main:app --host 0.0.0.0 --port 3000 --workers 1