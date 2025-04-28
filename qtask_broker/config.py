import os
import logging
from dotenv import load_dotenv

# Basic logging configuration (optional but recommended)
# Ensure logging is configured only once, preferably at the application entry point
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__) # Get logger for this module

class ConfigurationLoader:
    """
    Loads, validates, and stores application configuration from environment
    variables, prioritizing environment-specific .env files
    (e.g., .env.development, .env.production) over a base .env file.
    """
    def __init__(self, default_env='development'):
        """
        Initializes the configuration loader.

        Args:
            default_env (str): The environment to assume if APP_ENV is not defined.
                               Defaults to 'development'.
        """
        logger.info("Starting environment-based configuration loading...")

        # 1. Determine the environment (APP_ENV)
        app_env = os.environ.get('APP_ENV', default_env).lower()
        logger.info(f"Application environment detected (APP_ENV): '{app_env}'")

        # 2. Load the base .env file (if it exists) - Do not override existing variables
        base_dotenv_path = '.env'
        if os.path.exists(base_dotenv_path):
            logger.info(f"Loading base configuration from: {base_dotenv_path}")
            load_dotenv(dotenv_path=base_dotenv_path, override=False)
        else:
            logger.info(f"Base file {base_dotenv_path} not found, skipping.")

        # 3. Load the environment-specific .env file (if it exists) - Override base
        env_specific_dotenv_path = f".env.{app_env}"
        if os.path.exists(env_specific_dotenv_path):
            logger.info(f"Loading and overriding with specific configuration from: {env_specific_dotenv_path}")
            # override=True allows this file to overwrite values from .env or the environment
            load_dotenv(dotenv_path=env_specific_dotenv_path, override=True)
        else:
            logger.info(f"Environment-specific file {env_specific_dotenv_path} not found, skipping.")

        logger.info("Environment variables loaded. Proceeding to read values...")

        # --- Read and parse variables ---

        # --- Server Port ---
        default_port = 3000
        port_str = os.environ.get('PORT', str(default_port))
        try:
            self.port: int = int(port_str)
            logger.info(f"Server port: {self.port}")
        except ValueError:
            logger.warning(f"PORT value ('{port_str}') is not a valid integer. Using default: {default_port}")
            self.port = default_port

        # --- Redis Configuration ---
        self.redis_host: str = os.environ.get('REDIS_HOST', 'localhost')
        default_redis_port = 6379
        redis_port_str = os.environ.get('REDIS_PORT', str(default_redis_port))
        try:
            self.redis_port: int = int(redis_port_str)
        except ValueError:
            logger.warning(f"REDIS_PORT value ('{redis_port_str}') is not a valid integer. Using default: {default_redis_port}")
            self.redis_port = default_redis_port

        self.redis_username: str | None = os.environ.get('REDIS_USERNAME')
        self.redis_password: str | None = os.environ.get('REDIS_PASSWORD')

        logger.info(f"Redis Configuration: Host={self.redis_host}, Port={self.redis_port}, User={self.redis_username or 'N/A'}")


        # --- Partition Configuration ---
        default_num_partitions = 10
        # Get value AFTER load_dotenv has acted
        num_partitions_str = os.environ.get('NUM_PARTITIONS', str(default_num_partitions))
        logger.info(f"[Debug Env] NUM_PARTITIONS read from environment: '{num_partitions_str}' (Type: {type(num_partitions_str)})")
        try:
            # IMPORTANT: Try to parse the value read from the environment
            num_partitions_int = int(num_partitions_str)
            if num_partitions_int <= 0:
                 logger.warning(f"NUM_PARTITIONS must be > 0 (was '{num_partitions_int}'). Using default: {default_num_partitions}")
                 self.num_partitions: int = default_num_partitions
            else:
                 self.num_partitions: int = num_partitions_int
        except ValueError:
            logger.warning(f"NUM_PARTITIONS value ('{num_partitions_str}') is not a valid integer. Using default: {default_num_partitions}")
            self.num_partitions = default_num_partitions

        logger.info(f"[Debug Parse] NUM_PARTITIONS parsed: {self.num_partitions} (Type: {type(self.num_partitions)})")

        # --- Heartbeat Configuration ---
        default_ttl = 30
        ttl_str = os.environ.get('HEARTBEAT_TTL_SECONDS', str(default_ttl))
        try:
            ttl_int = int(ttl_str)
            if ttl_int <= 0:
                logger.warning(f"HEARTBEAT_TTL_SECONDS must be > 0 (was '{ttl_int}'). Using default: {default_ttl}")
                self.heartbeat_ttl_seconds: int = default_ttl
            else:
                self.heartbeat_ttl_seconds: int = ttl_int
        except ValueError:
            logger.warning(f"HEARTBEAT_TTL_SECONDS value ('{ttl_str}') is not a valid integer. Using default: {default_ttl}")
            self.heartbeat_ttl_seconds = default_ttl

        # The prefix was constant in your JS code, keeping it the same
        self.heartbeat_key_prefix: str = 'heartbeat:'

        logger.info(f"Heartbeat TTL: {self.heartbeat_ttl_seconds} seconds")
        logger.info(f"Heartbeat Key Prefix: '{self.heartbeat_key_prefix}'")
        logger.info("Configuration loaded and parsed successfully.")

# --- Example Usage (How to set the environment) ---
if __name__ == "__main__":
    # Configure basic logging for the example
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

    print("\n--- Running with APP_ENV undefined (will use default 'development') ---")
    # Simulate APP_ENV not being set
    if 'APP_ENV' in os.environ: del os.environ['APP_ENV']
    # Create example .env files (this will create them on your disk!)
    try:
        with open('.env', 'w') as f: f.write("PORT=9000\nREDIS_HOST=base_host\n")
        with open('.env.development', 'w') as f: f.write("PORT=3001\nNUM_PARTITIONS=5\n") # Overrides PORT, adds NUM_PARTITIONS
        with open('.env.production', 'w') as f: f.write("PORT=80\nREDIS_HOST=prod_host\nNUM_PARTITIONS=50\n")
    except IOError as e:
        logger.error(f"Could not write example .env files: {e}")


    print("\n--- Loading config (should use .env and .env.development) ---")
    try:
        config_dev = ConfigurationLoader(default_env='development')
        print(f"DEV Port: {config_dev.port}") # Expected: 3001 (from .env.development)
        print(f"DEV Redis Host: {config_dev.redis_host}") # Expected: base_host (from .env)
        print(f"DEV Partitions: {config_dev.num_partitions}") # Expected: 5 (from .env.development)
    except ValueError as e:
        logger.error(f"Error loading dev config: {e}")


    print("\n--- Running with APP_ENV=production ---")
    os.environ['APP_ENV'] = 'production'
    print("\n--- Loading config (should use .env and .env.production) ---")
    try:
        config_prod = ConfigurationLoader() # APP_ENV is now set in environment
        print(f"PROD Port: {config_prod.port}") # Expected: 80 (from .env.production)
        print(f"PROD Redis Host: {config_prod.redis_host}") # Expected: prod_host (from .env.production)
        print(f"PROD Partitions: {config_prod.num_partitions}") # Expected: 50 (from .env.production)
    except ValueError as e:
        logger.error(f"Error loading prod config: {e}")


    # Clean up example .env files and environment variable
    # try:
    #     if os.path.exists('.env'): os.remove('.env')
    #     if os.path.exists('.env.development'): os.remove('.env.development')
    #     if os.path.exists('.env.production'): os.remove('.env.production')
    # except IOError as e:
    #      logger.warning(f"Could not remove example .env files: {e}")
    # finally:
    #      if 'APP_ENV' in os.environ: del os.environ['APP_ENV']
    print("\nExample .env files created. Please delete them manually if necessary.")

