import json
import hashlib
import os
import re
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

class FileHandler:
    """Class for handling file operations like loading and saving JSON files."""

    @staticmethod
    def load_json_file(file_path):
        """Load JSON data from a file."""
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error reading JSON file {file_path}: {e}")
            raise

    @staticmethod
    def save_json_file(data, output_path):
        """Save JSON data to a file."""
        try:
            with open(output_path, 'w') as f:
                json.dump(data, f, indent=4)
            logging.info(f"Data successfully saved to {output_path}")
        except Exception as e:
            logging.error(f"Failed to save data to {output_path}: {e}")


class DataProcessor:
    """Class for processing the data and merging hierarchical data."""

    @staticmethod
    def generate_id(user_story):
        """Generate a unique ID for a user story based on its content."""
        story_content = json.dumps({
            "User Story": user_story.get("User Story", ""),
            "Acceptance Criteria": user_story.get("Acceptance Criteria", []),
            "Common Bugs": user_story.get("Common Bugs", {"Functional": [], "Non-Functional": []})
        }, sort_keys=True)
        return hashlib.sha256(story_content.encode()).hexdigest()

    @staticmethod
    def merge_hierarchical_data(existing_data, new_data):
        """Merge new hierarchical data into existing data."""
        for new_subdomain in new_data["Subdomains"]:
            existing_subdomain = next(
                (sd for sd in existing_data["Subdomains"] if sd["Subdomain Name"] == new_subdomain["Subdomain Name"]), None)

            if not existing_subdomain:
                existing_data["Subdomains"].append(new_subdomain)
            else:
                for new_region in new_subdomain["Regions"]:
                    existing_region = next(
                        (r for r in existing_subdomain["Regions"] if r["Region Name"] == new_region["Region Name"]), None)

                    if not existing_region:
                        existing_subdomain["Regions"].append(new_region)
                    else:
                        for new_platform in new_region["Platforms"]:
                            existing_platform = next((p for p in existing_region["Platforms"] if
                                                      p["Platform Name"] == new_platform["Platform Name"]), None)

                            if not existing_platform:
                                existing_region["Platforms"].append(new_platform)
                            else:
                                for new_software in new_platform["Software Types"]:
                                    existing_software = next(
                                        (st for st in existing_platform["Software Types"] if
                                         st["Software Type Name"] == new_software["Software Type Name"]),
                                        None
                                    )

                                    if not existing_software:
                                        existing_platform["Software Types"].append(new_software)
                                    else:
                                        for req_type in ["Functional", "Non-Functional"]:
                                            existing_requirements = existing_software["Requirements"].get(req_type, [])
                                            new_requirements = new_software["Requirements"].get(req_type, [])

                                            for new_feature in new_requirements:
                                                existing_feature = next(
                                                    (f for f in existing_requirements if
                                                     f["Feature Name"] == new_feature["Feature Name"]),
                                                    None
                                                )

                                                if not existing_feature:
                                                    existing_requirements.append(new_feature)
                                                else:
                                                    existing_feature["User Stories"].extend(
                                                        us for us in new_feature["User Stories"] if
                                                        us["_id"] not in {e["_id"] for e in
                                                                          existing_feature["User Stories"]}
                                                    )
        return existing_data

    @staticmethod
    def process_data(input_data, domain_name):
        """Process the input data into the desired output format."""
        output_data = {
            "Domain": domain_name,
            "Subdomains": []
        }

        for entry in input_data:
            metadata = entry.get("metadata", {})
            subdomain_name = metadata.get("Subdomain", "Unknown Subdomain")
            platform_name = metadata.get("Platform", "Unknown Platform")
            software_type_name = metadata.get("Software Type", "Unknown Software Type")
            requirement_type = metadata.get("Requirement Type", "Unknown Requirement Type").strip().upper()

            data = entry.get("data", {})
            feature_names = data.get("Feature Name", [])
            if not feature_names:
                continue

            subdomain = next((sd for sd in output_data["Subdomains"] if sd["Subdomain Name"] == subdomain_name), None)
            if not subdomain:
                subdomain = {
                    "Subdomain Name": subdomain_name,
                    "Regions": []
                }
                output_data["Subdomains"].append(subdomain)

            for user_story in entry["data"].get("User Stories", []):
                apps = user_story.get("App Names", {})
                combined_apps = {region: apps_list for region, apps_list in apps.items()}
                user_story_id = DataProcessor.generate_id(user_story)

                for region in apps.keys():
                    region_obj = next((r for r in subdomain["Regions"] if r["Region Name"] == region), None)
                    if not region_obj:
                        region_obj = {
                            "Region Name": region,
                            "Platforms": []
                        }
                        subdomain["Regions"].append(region_obj)

                    platform_obj = next((p for p in region_obj["Platforms"] if p["Platform Name"] == platform_name), None)
                    if not platform_obj:
                        platform_obj = {
                            "Platform Name": platform_name,
                            "Software Types": []
                        }
                        region_obj["Platforms"].append(platform_obj)

                    software_type_obj = next(
                        (st for st in platform_obj["Software Types"] if st["Software Type Name"] == software_type_name),
                        None)
                    if not software_type_obj:
                        software_type_obj = {
                            "Software Type Name": software_type_name,
                            "Requirements": {
                                "Functional": [],
                                "Non-Functional": []
                            }
                        }
                        platform_obj["Software Types"].append(software_type_obj)

                    requirement_list = software_type_obj["Requirements"].get(
                        "Functional" if requirement_type == "FR" else "Non-Functional", []
                    )

                    feature_obj = next((f for f in requirement_list if f["Feature Name"] == feature_names), None)
                    if not feature_obj:
                        feature_obj = {
                            "Feature Name": feature_names,
                            "User Stories": []
                        }
                        requirement_list.append(feature_obj)

                    feature_obj["User Stories"].append({
                        "_id": user_story_id,
                        "Quality": user_story.get("Quality", "Unknown Quality"),
                        "User Story": user_story.get("User Story", "Unknown User Story"),
                        "Acceptance Criteria": user_story.get("Acceptance Criteria", []),
                        "Common Bugs": user_story.get("Common Bugs", {"Functional": [], "Non-Functional": []}),
                        "apps": combined_apps,
                        "data_type": "Synthetic Data"
                    })

        return output_data


class DomainProcessor:
    """Main class to coordinate domain-specific data processing."""

    def __init__(self, input_dir, output_dir, raw_data_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.raw_data_dir = raw_data_dir

    def process_files(self):
        """Main method to process input files."""

        input_files = [os.path.join(self.input_dir, f) for f in os.listdir(self.input_dir) if f.startswith('LLM_org') and f.endswith('.json')]
        domain_files = [os.path.join(self.raw_data_dir, f) for f in os.listdir(self.raw_data_dir) if f.endswith('.json')]

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        file_to_domain_map = {}
        for f in input_files:
            match = re.search(r"org_(.*?)_appdata\.json", f)
            if match:
                file_to_domain_map[os.path.basename(f)] = match.group(1)
            else:
                logging.warning(f"Filename {os.path.basename(f)} does not match the expected pattern.")

        for input_file in input_files:
            try:
                base_name = os.path.basename(input_file)
                domain_name = file_to_domain_map.get(base_name, "Unknown Domain")
                logging.info(f"Processing file: {input_file} with domain: {domain_name}")

                input_data = FileHandler.load_json_file(input_file)
                processed_data = DataProcessor.process_data(input_data, domain_name)

                # Save processed data to output file
                output_file_name = f"{self.output_dir}/Merged_{base_name}"
                FileHandler.save_json_file(processed_data, output_file_name)

            except Exception as e:
                logging.error(f"Error processing {input_file}: {e}")



def Matter2FormattedData(input_dir,output_dir,raw_data_dir):
    """Main function to initialize the DomainProcessor and start processing."""
    logging.info("__Matter2Formatted Sequence Initiated__")
    processor = DomainProcessor(
        input_dir,
        output_dir,
        raw_data_dir
    )
    processor.process_files()
    logging.info("__Matter2Formatted Sequence Executed__\n")