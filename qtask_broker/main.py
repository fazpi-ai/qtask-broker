import logging
from contextlib import asynccontextmanager
from typing import List, Dict, Any

# Web framework and data validation
from fastapi import FastAPI, HTTPException, Body, status
from pydantic import BaseModel, Field # Field for extra validation

# Import our business logic classes
from config import ConfigurationLoader
from redis_manager import RedisManager
from partition_manager import PartitionManager
from assignment_service import AssignmentService
from subscription_service import SubscriptionService
from publishing_service import PublishingService
from topic_service import TopicService

# --- Logging Configuration ---
# Configure once at the application's entry point
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Configuration Loading and Service Instantiation ---
# This runs once when the FastAPI application starts

try:
    logger.info("Loading global configuration...")
    config = ConfigurationLoader() # Reads APP_ENV and loads .env.*

    logger.info("Initializing RedisManager...")
    redis_manager = RedisManager(config)
    # Attempt to connect once at startup (optional, but good for early problem detection)
    # _ensure_connection will be called anyway before each operation
    if not redis_manager._ensure_connection():
         logger.warning("Initial Redis connection failed. The application will attempt to reconnect on requests.")
         # You could decide to exit here if the initial connection is critical:
         # raise RuntimeError("Could not establish initial Redis connection.")

    logger.info("Initializing other services...")
    partition_manager = PartitionManager(config)
    assignment_service = AssignmentService(redis_manager)
    subscription_service = SubscriptionService(redis_manager, partition_manager)
    publishing_service = PublishingService(redis_manager, partition_manager)
    topic_service = TopicService(redis_manager)

    logger.info("All services initialized.")

except ValueError as e:
    logger.error(f"Fatal error during initialization due to invalid configuration: {e}", exc_info=True)
    # Exit if basic configuration (e.g., num_partitions) is invalid
    raise SystemExit(f"Configuration error: {e}") from e
except Exception as e:
    logger.error(f"Fatal error during service initialization: {e}", exc_info=True)
    raise SystemExit(f"Initialization error: {e}") from e


# --- Application Lifespan ---
# To handle actions on startup and shutdown (e.g., closing Redis connection)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Code to run before the application starts receiving requests
    logger.info("Application starting up. Checking Redis connection...")
    # Use a direct ping within lifespan if possible, or rely on the check during init
    try:
        if not redis_manager.redis_client or not redis_manager.redis_client.ping():
             logger.warning("Redis connection not active at startup, will attempt to reconnect.")
        else:
             logger.info("Redis connection active at startup.")
    except Exception:
         logger.warning("Could not verify Redis connection at startup, will attempt to reconnect.")
    yield # The application runs here
    # Code to run after the application stops receiving requests
    logger.info("Application shutting down. Closing Redis connection...")
    redis_manager.close_connection()
    logger.info("Redis connection closed.")


# --- Create FastAPI Application ---
# Updated title and description
app = FastAPI(
    title="QTask Broker API",
    description="Centralized API for managing partitioned queues with Redis Streams (QTask).",
    version="1.0.0", # You might want to update the version
    lifespan=lifespan # Register the lifespan handler
)

# --- Data Models (Pydantic) for Request Validation ---
# Field descriptions updated to English

class AssignPartitionRequest(BaseModel):
    topic: str = Field(..., min_length=1, description="Base topic name.")
    group: str = Field(..., min_length=1, description="Consumer group name.")
    consumerId: str = Field(..., min_length=1, description="Unique consumer ID.")

class AssignPartitionResponse(BaseModel):
    success: bool
    partitionIndex: int

class SubscribeRequest(BaseModel):
    topic: str = Field(..., min_length=1)
    group: str = Field(..., min_length=1)
    partitionIndex: int = Field(..., ge=0, description="Index of the partition to subscribe to (must be >= 0).")

class SubscribeResponse(BaseModel):
    success: bool
    message: str

class PushRequest(BaseModel):
    topic: str = Field(..., min_length=1)
    partitionKey: str = Field(..., min_length=1, description="Key used to determine the partition.")
    data: Dict[str, Any] # Accepts any valid JSON dictionary

class PushResponse(BaseModel):
    success: bool
    topic: str
    partition: int
    messageId: str

class TopicsResponse(BaseModel):
    topics: List[str]


# --- API Endpoints ---

@app.post(
    "/assign-partition",
    response_model=AssignPartitionResponse,
    status_code=status.HTTP_200_OK,
    summary="Assigns a partition to a consumer", # Updated summary
    tags=["Assignments"]
)
async def assign_partition_endpoint(request: AssignPartitionRequest):
    """
    Atomically attempts to assign an available partition of a specific topic/group
    to a consumer using a Lua script. Checks heartbeats to reassign partitions
    from inactive consumers.
    """
    logger.info(f"Received POST /assign-partition request: {request.model_dump()}")
    assigned_partition = assignment_service.assign_partition(
        topic=request.topic,
        group=request.group,
        consumer_id=request.consumerId
    )

    if assigned_partition >= 0:
        return AssignPartitionResponse(success=True, partitionIndex=assigned_partition)
    else:
        # Return 409 Conflict if no partitions are available
        logger.warning(f"No available partitions found for {request.consumerId} in {request.topic}/{request.group}")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"No available partitions found for topic '{request.topic}', group '{request.group}'."
        )

@app.post(
    "/subscribe",
    response_model=SubscribeResponse,
    status_code=status.HTTP_200_OK,
    summary="Ensures a group subscription to a partition", # Updated summary
    tags=["Subscriptions"]
)
async def subscribe_endpoint(request: SubscribeRequest):
    """
    Ensures the partitioned stream and consumer group exist in Redis.
    Creates the stream if it doesn't exist (MKSTREAM). Registers the base topic.
    Idempotent.
    """
    logger.info(f"Received POST /subscribe request: {request.model_dump()}")

    # Validate partitionIndex against current config's num_partitions
    # (PartitionManager also does this, but good to validate at the API layer)
    if request.partitionIndex >= config.num_partitions:
         logger.error(f"Invalid partitionIndex {request.partitionIndex} for num_partitions={config.num_partitions}")
         raise HTTPException(
             status_code=status.HTTP_400_BAD_REQUEST,
             detail=f"Invalid partitionIndex: {request.partitionIndex}. Must be less than {config.num_partitions}."
         )

    success = subscription_service.ensure_subscription(
        topic=request.topic,
        group=request.group,
        partition_index=request.partitionIndex
    )

    if success:
        return SubscribeResponse(
            success=True,
            message=f"Subscription for group '{request.group}' on partition {request.partitionIndex} of topic '{request.topic}' ensured."
        )
    else:
        # If the service returns False, it indicates an unrecoverable error
        logger.error(f"Internal error ensuring subscription for {request.topic}/{request.group} on partition {request.partitionIndex}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error processing subscription."
        )

@app.post(
    "/push",
    response_model=PushResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Publishes a message to a topic partition", # Updated summary
    tags=["Publishing"]
)
async def push_endpoint(request: PushRequest):
    """
    Calculates the target partition based on the `partitionKey`, serializes
    the `data` to JSON, and publishes the message to the corresponding Redis stream.
    Registers the base topic.
    """
    # Don't log full data by default for potentially large payloads
    logger.info(f"Received POST /push request: topic={request.topic}, partitionKey={request.partitionKey}, data_keys={list(request.data.keys())}")

    success, partition_index, message_id = publishing_service.publish_message(
        topic=request.topic,
        partition_key=request.partitionKey,
        data=request.data
    )

    if success and partition_index is not None and message_id is not None:
        return PushResponse(
            success=True,
            topic=request.topic,
            partition=partition_index,
            messageId=message_id
        )
    else:
        # Determine failure cause (although the service already logged it)
        # We could return more specific errors if the service provided them
        logger.error(f"Error processing /push for topic {request.topic}, key {request.partitionKey}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error processing push request."
            # Could add more details if provided by the service,
            # e.g., "JSON serialization error", "Redis connection error"
        )


@app.get(
    "/topics",
    response_model=TopicsResponse,
    status_code=status.HTTP_200_OK,
    summary="Gets the list of managed base topics", # Updated summary
    tags=["Topics"]
)
async def get_topics_endpoint():
    """
    Returns a list of all base topic names that have been registered
    (via /subscribe or /push).
    """
    logger.info("Received GET /topics request")
    topics_list = topic_service.get_managed_topics()
    # The service already handles the error case by returning [], which is acceptable for the API
    return TopicsResponse(topics=topics_list)


# --- Entry point for running with Uvicorn (optional) ---
# This allows running with 'python main.py' but using 'uvicorn main:app --reload' is better
if __name__ == "__main__":
    import uvicorn
    logger.info(f"Starting Uvicorn server on http://localhost:{config.port}")
    # Use 0.0.0.0 to bind to all interfaces, making it accessible externally if needed
    uvicorn.run(app, host="0.0.0.0", port=config.port)
    # Note: 'reload=True' is useful for development but better passed via command line.
    # uvicorn.run("main:app", host="0.0.0.0", port=config.port, reload=True)
