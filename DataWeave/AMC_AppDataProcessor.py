import json
import hashlib
from collections import defaultdict
import os
import logging

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

class FileProcessor:
    """Class to handle file operations like hashing and loading data."""

    @staticmethod
    def calculate_file_hash(filename):
        """Calculate the MD5 hash of a file."""
        logging.debug(f"Calculating hash for file: {filename}")
        hasher = hashlib.md5()
        with open(filename, "rb") as file:
            hasher.update(file.read())
        return hasher.hexdigest()

    @staticmethod
    def has_file_changed(filename, hash_file_suffix=".hash", processed_folder="ProcessedAppData"):
        """Check if the file content has changed by comparing hashes."""
        logging.debug(f"Checking if file has changed: {filename}")
        hash_file = os.path.join(processed_folder, f"{os.path.basename(filename)}{hash_file_suffix}")
        current_hash = FileProcessor.calculate_file_hash(filename)

        if os.path.exists(hash_file):
            with open(hash_file, "r") as file:
                saved_hash = file.read().strip()
                if current_hash == saved_hash:
                    logging.info(f"No changes detected for file: {filename}")
                    return False

        with open(hash_file, "w") as file:
            file.write(current_hash)
        logging.info(f"Changes detected for file: {filename}")
        return True

    @staticmethod
    def load_input_file(filename):
        """Load JSON data from a file."""
        logging.info(f"Loading input file: {filename}")
        with open(filename, "r") as file:
            return json.load(file)

    @staticmethod
    def save_to_file(data, filename):
        """Save JSON data to a file."""
        logging.info(f"Saving processed data to file: {filename}")
        with open(filename, "w") as file:
            json.dump(data, file, indent=4)
        logging.info(f"Data saved successfully to: {filename}")


class AppDataOrganizer:
    """Class to organize application data into structured JSON."""

    @staticmethod
    def organize_apps(data):
        """Organize the input JSON data by subdomains, regions, platforms, and layers."""
        logging.info("Organizing application data.")
        organized_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))

        for subdomain in data.get("Subdomains", []):
            subdomain_name = subdomain.get("Subdomain Name", "Unknown Subdomain")
            for region in subdomain.get("Regions", []):
                region_name = region.get("Region", "Unknown Region")
                for app in region.get("Apps", []):
                    app_name = app["name"]
                    logging.debug(f"Processing app: {app_name}")

                    # Process Functional Requirements
                    functional_requirements = app.get("functional_requirements", {})
                    AppDataOrganizer._process_requirements(
                        organized_data, subdomain_name, region_name, app_name,
                        functional_requirements, "FR", app
                    )

                    # Process Non-Functional Requirements
                    non_functional_requirements = app.get("non_functional_requirements", {})
                    AppDataOrganizer._process_requirements(
                        organized_data, subdomain_name, region_name, app_name,
                        non_functional_requirements, "NFR", app
                    )

        logging.info("Application data organized successfully.")
        return json.loads(json.dumps(organized_data))

    @staticmethod
    def _process_requirements(organized_data, subdomain_name, region_name, app_name, requirements, req_type, app):
        """Helper function to process requirements."""
        for platform, layers in requirements.items():
            for layer, features in layers.items():
                for feature in features:
                    if req_type not in organized_data[subdomain_name][platform][layer]:
                        organized_data[subdomain_name][platform][layer][req_type] = []
                    feature_found = False
                    for item in organized_data[subdomain_name][platform][layer][req_type]:
                        if item["Feature"] == feature:
                            if region_name not in item["apps"]:
                                item["apps"][region_name] = []
                            item["apps"][region_name].append(app_name)
                            AppDataOrganizer._add_optional_data(item, app)
                            feature_found = True
                            break
                    if not feature_found:
                        new_feature = {
                            "Feature": feature,
                            "apps": {region_name: [app_name]},
                        }
                        AppDataOrganizer._add_optional_data(new_feature, app)
                        organized_data[subdomain_name][platform][layer][req_type].append(new_feature)

    @staticmethod
    def _add_optional_data(item, app):
        """Add optional data like acceptance criteria or common bugs."""
        if "acceptance_criteria" in app:
            item["acceptance_criteria"] = app["acceptance_criteria"]
        if "common_bugs" in app:
            item["common_bugs"] = app["common_bugs"]


def AppDataProcessor(input_dir, output_dir):
    """Main function to process application data files."""
    logging.info("__AppDataProcessor Sequence Initiated__")

    # Ensure the output directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logging.debug(f"Created output directory: {output_dir}")

    input_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if
                   os.path.isfile(os.path.join(input_dir, f)) and f.endswith('_appdata.json')]

    for input_file in input_files:
        output_file = os.path.join(output_dir, f"org_{os.path.basename(input_file)}")

        if not FileProcessor.has_file_changed(input_file):
            logging.info(f"Skipping unchanged file: {input_file}")
        else:
            logging.info(f"Processing file: {input_file}")
            json_data = FileProcessor.load_input_file(input_file)
            organized_data = AppDataOrganizer.organize_apps(json_data)
            FileProcessor.save_to_file(organized_data, output_file)

    logging.info("__AppDataProcessor Sequence Executed__\n")