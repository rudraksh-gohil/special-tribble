import json
import os
import logging
from pymongo import MongoClient

log_directory = "log"
os.makedirs(log_directory, exist_ok=True)

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_directory, "execution.log"), mode="a")
    ]
)


class MongoDBHandler:
    """Handles MongoDB connections and operations."""

    def __init__(self, mongo_uri, database_name):
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.client = None
        self.db = None

    def connect(self):
        """Attempts to connect to the MongoDB database."""
        try:
            # For local MongoDB, remove TLS settings
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.database_name]
            logging.info("Successfully connected to MongoDB.")
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            self.db = None

    def save_to_collection(self, collection_name, data):
        """Saves data to the specified MongoDB collection."""
        if self.db is None:  # Explicit comparison to None
            logging.error("No database connection. Cannot save data.")
            return

        try:
            collection = self.db[collection_name]
            collection.insert_one(data)
            logging.info(f"Data successfully saved to the '{collection_name}' collection.")
        except Exception as e:
            logging.error(f"Failed to save data to MongoDB: {e}")


class JSONFileHandler:
    """Handles reading JSON data from files."""

    @staticmethod
    def read_json(file_path):
        """Reads JSON data from the given file path."""
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
                logging.info(f"Successfully read JSON data from {file_path}")
                return data
        except Exception as e:
            logging.error(f"Failed to read JSON file {file_path}: {e}")
            return None


class MongoDataUploadeR:
    """Coordinates the process of uploading JSON data to MongoDB."""

    def __init__(self, db_handler, schema_dir):
        self.db_handler = db_handler
        self.schema_dir = schema_dir

    def upload_data(self, collection_name):
        """Reads all JSON files starting with 'FOR_VAR' from the schema directory and uploads data to MongoDB."""
        if not os.path.exists(self.schema_dir):
            logging.error(f"Schema directory '{self.schema_dir}' does not exist.")
            return

        for file_name in os.listdir(self.schema_dir):
            # Process only files that start with 'FOR_VAR' and have a .json extension
            if file_name.startswith('FOR_VAR') and file_name.endswith('.json'):
                file_path = os.path.join(self.schema_dir, file_name)
                data = JSONFileHandler.read_json(file_path)

                if data:
                    self.db_handler.save_to_collection(collection_name, data)
                    logging.info(f"Data from {file_name} uploaded to MongoDB.")


def SYS_MongoDataUploader(MONGO_URI, DATABASE_NAME, FORMATTED_SCHEMA_DIR):
    """Main function to initialize classes and run the data upload process."""
    logging.info("__MongoDataUploader Sequence Initiated__")

    # Initialize MongoDB handler and connect to database
    db_handler = MongoDBHandler(MONGO_URI, DATABASE_NAME)
    db_handler.connect()

    if db_handler.db is not None:  # Explicit comparison to None
        # Initialize the uploader and process JSON files
        uploader = MongoDataUploadeR(db_handler, FORMATTED_SCHEMA_DIR)
        uploader.upload_data("data")
    else:
        logging.error("Database connection not established. Aborting process.")

    logging.info("__MongoDataUploader Sequence Executed\n")