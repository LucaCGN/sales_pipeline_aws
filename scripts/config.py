# scripts/config.py 

import logging
import os
import openai

class Config:
    """
    Configuration class to manage application settings, logging, and external service integrations.
    """
    def __init__(self):
        # Get the directory of this file
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))

        # Define essential paths emulating AWS service directories
        self.LOGS_PATH = os.path.abspath(os.path.join(self.BASE_DIR, '..', 'logs'))
        self.BUCKET_PATH = os.path.abspath(os.path.join(self.BASE_DIR, '..', 'bucket'))
        self.DATABASE_PATH = os.path.abspath(os.path.join(self.BASE_DIR, '..', 'database', 'sales.db'))
        self.TEMP_DATA_PATH = os.path.abspath(os.path.join(self.BASE_DIR, '..', 'temp_data'))

        # Ensure the necessary directories exist
        os.makedirs(self.LOGS_PATH, exist_ok=True)
        os.makedirs(self.BUCKET_PATH, exist_ok=True)
        os.makedirs(os.path.dirname(self.DATABASE_PATH), exist_ok=True)
        os.makedirs(self.TEMP_DATA_PATH, exist_ok=True)

        # Setup loggers first
        self.database_logger = self.setup_logger('database_logger', 'database_operations.log')
        self.pipeline_logger = self.setup_logger('pipeline_logger', 'pipeline_execution.log')
        self.app_logger = self.setup_logger('app_logger', 'app.log')  # Logger for application events

        # Retrieve OpenAI API key from environment variables for security
        self.OPENAI_API_KEY = "your-api-key"

        if not self.OPENAI_API_KEY:
            self.app_logger.error("OPENAI_API_KEY environment variable is not set.")
            raise ValueError("OPENAI_API_KEY environment variable is not set.")

        # Initialize OpenAI client after setting up loggers
        self.client = self.setup_openai_client()

    def setup_openai_client(self):
        """
        Initialize the OpenAI client with the provided API key.
        """
        openai.api_key = self.OPENAI_API_KEY
        self.app_logger.info("OpenAI client initialized successfully.")
        return openai

    def setup_logger(self, logger_name, log_file):
        """
        Set up a logger with the specified name and log file.
        """
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)

        # Create file handler for logging
        handler = logging.FileHandler(os.path.join(self.LOGS_PATH, log_file))
        handler.setLevel(logging.INFO)

        # Define log message format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        # Add handler to the logger if not already present
        if not logger.handlers:
            logger.addHandler(handler)

        return logger

# Initialize the configuration instance for use across the project
config = Config()


