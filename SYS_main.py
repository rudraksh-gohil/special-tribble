import logging
import os
import sys
from DataWeave.SYS_UserStoryGeneration import SYS_UserStoryGenration
from DataWeave.SYS_Matter2FormattedData import SYS_Matter2FormattedData
from DataWeave.SYS_MongoDataUploader import SYS_MongoDataUploader
from DataWeave.SYS_Neo4jDataUploader import SYS_Neo4jDataUploader

# Setup logging
log_directory = "log"
os.makedirs(log_directory, exist_ok=True)

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_directory, "execution.log"), mode="a")
    ]
)

# Function to execute each step
def run_steps():
    while True:
 # Infinite loop to keep running until error or manual stop
        try:

            # Step 1 -> UserStory Generation
            try:
                SYS_UserStoryGenration()  # This is where you check for rate limits
            except KeyboardInterrupt:
                logging.info("Script execution interrupted during UserStoryGeneration step. Exiting gracefully.")
                sys.exit(0)  # Exit if interrupted during this step
            except Exception as e:
                if "All models have exhausted their rate limits" in str(e):
                    logging.error("All models have exhausted their rate limits. Stopping execution.")
                    sys.exit(0)  # Exit if rate limits are exhausted
                else:
                    logging.error(f"Error during UserStoryGeneration: {e}")
                    sys.exit(1)  # Exit with error if this step fails

            # Step 3 -> Synthetic Data Formatter
            llm_input_dir = "LLM_Processed_Files"
            formatter_output = "Formatted_Schema_Files"
            try:
                SYS_Matter2FormattedData()
            except KeyboardInterrupt:
                logging.info("Script execution interrupted during Matter2FormattedData step. Exiting gracefully.")
                sys.exit(0)  # Exit if interrupted during this step
            except Exception as e:
                logging.error(f"Error during Matter2FormattedData: {e}")
                sys.exit(1)  # Exit with error if this step fails

            # Step 4 -> Data Pushing to MongoDB
            mongo_uri = "mongodb://127.0.0.1:27017"
            database_name = "UserStoryData"
            try:
                SYS_MongoDataUploader(mongo_uri, database_name, formatter_output)
            except KeyboardInterrupt:
                logging.info("Script execution interrupted during MongoDataUploader step. Exiting gracefully.")
                sys.exit(0)  # Exit if interrupted during this step
            except Exception as e:
                logging.error(f"Error during MongoDataUploader: {e}")
                sys.exit(1)  # Exit with error if this step fails

            # Step 5 -> Data Pushing to Neo4j
            neo4j_uri = "bolt://localhost:7687"
            neo4j_auth = ("neo4j", "rudy2004")
            try:
                SYS_Neo4jDataUploader(neo4j_uri, neo4j_auth, formatter_output)
            except KeyboardInterrupt:
                logging.info("Script execution interrupted during Neo4jDataUploader step. Exiting gracefully.")
                sys.exit(0)  # Exit if interrupted during this step
            except Exception as e:
                logging.error(f"Error during Neo4jDataUploader: {e}")
                sys.exit(1)  # Exit with error if this step fails

        except KeyboardInterrupt:
            logging.info("Script execution interrupted by user (Ctrl+C). Exiting gracefully.")
            sys.exit(0)  # Exit on user interruption

        except Exception as e:
            logging.error(f"Unexpected error occurred: {e}")
            sys.exit(1)  # Exit with error if any unexpected error occurs

# Run the steps continuously
if __name__ == "__main__":
    run_steps()