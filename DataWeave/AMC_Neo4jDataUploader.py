import json
import os
import uuid
import time
import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

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

logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)
logger = logging.getLogger("neo4j")
logger.setLevel(logging.INFO)

class Neo4jConnection:
    """Handles connection to the Neo4j database."""

    def __init__(self):
        self.driver = None

    def connect(self, URI, AUTH):
        max_retries = 5
        retries = 0

        while retries < max_retries:
            try:
                self.driver = GraphDatabase.driver(URI, auth=AUTH)
                self.driver.verify_connectivity()
                logger.info("Successfully connected to Neo4j!")
                return
            except ServiceUnavailable:
                retries += 1
                logger.warning(f"Unable to retrieve routing information. Retrying ({retries}/{max_retries})...")
                time.sleep(5)
            except AuthError:
                logger.error("Authentication failed. Please check your credentials.")
                break
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")
                break
        logger.error("Failed to connect after multiple attempts.")

    def close(self):
        if self.driver:
            self.driver.close()

class IDGenerator:
    """Generates unique identifiers."""
    @staticmethod
    def generate():
        return str(uuid.uuid4())

class DataProcessor:
    """Processes data and interacts with the Neo4j database."""

    def __init__(self, driver):
        self.driver = driver

    def create_or_find_node(self, label, properties):
        query = f"MERGE (n:{label} {properties}) RETURN id(n) AS node_id"
        with self.driver.session() as session:
            result = session.run(query)
            return result.single()['node_id']

    def create_relationship(self, node1_id, node2_id, relationship_type):
        query = f"""
        MATCH (n1)
        WHERE id(n1) = $node1_id
        MATCH (n2)
        WHERE id(n2) = $node2_id
        MERGE (n1)-[r:{relationship_type}]->(n2)
        """
        with self.driver.session() as session:
            session.run(query, node1_id=node1_id, node2_id=node2_id)

    @staticmethod
    def convert_data_to_lowercase(data):
        if isinstance(data, dict):
            return {k.lower(): DataProcessor.convert_data_to_lowercase(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [DataProcessor.convert_data_to_lowercase(item) for item in data]
        elif isinstance(data, str):
            return data.lower()
        else:
            return data

    def process_data(self, data):
        data = self.convert_data_to_lowercase(data)

        domain_name = data["domain"]
        domain_properties = f'{{domain: "{domain_name}"}}'
        domain_node_id = self.create_or_find_node("Domain", domain_properties)

        for subdomain in data["subdomains"]:
            subdomain_name = subdomain["subdomain name"]
            subdomain_properties = f'{{subdomain_name: "{subdomain_name}"}}'
            subdomain_node_id = self.create_or_find_node("Subdomain", subdomain_properties)
            self.create_relationship(subdomain_node_id, domain_node_id, "BELONGS_TO_DOMAIN")

            for region in subdomain["regions"]:
                region_name = region["region name"]
                region_properties = f'{{region_name: "{region_name}"}}'
                region_node_id = self.create_or_find_node("Region", region_properties)
                self.create_relationship(region_node_id, subdomain_node_id, "BELONGS_TO_SUBDOMAIN")

                for platform in region["platforms"]:
                    platform_name = platform["platform name"]
                    platform_properties = f'{{platform_name: "{platform_name}"}}'
                    platform_node_id = self.create_or_find_node("Platform", platform_properties)
                    self.create_relationship(platform_node_id, region_node_id, "BELONGS_TO_REGION")

                    for software_type in platform["software types"]:
                        software_type_name = software_type["software type name"]
                        software_type_properties = f'{{software_type: "{software_type_name}"}}'
                        software_type_node_id = self.create_or_find_node("SoftwareType", software_type_properties)
                        self.create_relationship(software_type_node_id, platform_node_id, "BELONGS_TO_PLATFORM")

                        for req_type, requirements in software_type["requirements"].items():
                            requirement_type_properties = f'{{type: "{req_type}"}}'
                            requirement_type_node_id = self.create_or_find_node("RequirementType", requirement_type_properties)
                            self.create_relationship(requirement_type_node_id, software_type_node_id, "DEFINED_BY_SOFTWARE_TYPE")

                            for req in requirements:
                                for feature_name in req["feature name"]:
                                    feature_properties = f'{{feature_name: "{feature_name}"}}'
                                    feature_node_id = self.create_or_find_node("Feature", feature_properties)
                                    self.create_relationship(feature_node_id, software_type_node_id, "BELONGS_TO_SOFTWARE_TYPE")
                                    self.create_relationship(feature_node_id, requirement_type_node_id, "DEFINED_BY_REQUIREMENT_TYPE")

                                    for region_name_in_app, apps in req["user stories"][0]["apps"].items():
                                        for app in apps:
                                            if region_name_in_app == region_name:
                                                app_properties = f'{{app_name: "{app}"}}'
                                                app_node_id = self.create_or_find_node("AppName", app_properties)
                                                self.create_relationship(app_node_id, feature_node_id, "IS_A_FEATURE_OF")
                                                self.create_relationship(app_node_id, region_node_id, "AVAILABLE_IN_REGION")
                                                self.create_relationship(app_node_id, subdomain_node_id, "ASSOCIATED_WITH_SUBDOMAIN")

                                for user_story in req["user stories"]:
                                    user_story_text = user_story["user story"]
                                    user_quality = user_story["quality"]
                                    user_data_types = user_story.get("data_type", [])
                                    user_story_mongo = user_story.get("_id", [])
                                    user_story_properties = f'{{user_story: "{user_story_text}", data_types: {json.dumps(user_data_types)}, mDB_id: {json.dumps(user_story_mongo)}}}'
                                    user_story_node_id = self.create_or_find_node("UserStory", user_story_properties)

                                    quality_properties = f'{{quality: "{user_quality}"}}'
                                    quality_node_id = self.create_or_find_node("Quality", quality_properties)
                                    self.create_relationship(quality_node_id, user_story_node_id, "IS_QUALITY_OF")

                                    self.create_relationship(user_story_node_id, subdomain_node_id, "BELONGS_TO_SUBDOMAIN")
                                    self.create_relationship(user_story_node_id, platform_node_id, "BELONGS_TO_PLATFORM")
                                    self.create_relationship(user_story_node_id, feature_node_id, "IS_A_USER_STORY_FOR")
                                    self.create_relationship(user_story_node_id, requirement_type_node_id, "DERIVED_FROM_REQUIREMENT_TYPE")

                                    for acceptance_criteria in user_story["acceptance criteria"]:
                                        ac_properties = f'{{acceptance_criteria: "{acceptance_criteria}"}}'
                                        ac_node_id = self.create_or_find_node("AcceptanceCriteria", ac_properties)
                                        self.create_relationship(ac_node_id, user_story_node_id, "IS_CRITERIA_FOR")

                                    common_bugs = user_story["common bugs"]
                                    for bug_type, bugs in common_bugs.items():
                                        bug_type_properties = f'{{type: "{bug_type}"}}'
                                        bug_type_node_id = self.create_or_find_node("RequirementType", bug_type_properties)

                                        for bug in bugs:
                                            cb_properties = f'{{commonbugs: "{bug}"}}'
                                            cb_node_id = self.create_or_find_node("CommonBug", cb_properties)
                                            self.create_relationship(cb_node_id, user_story_node_id, "IS_A_COMMON_BUG_FOR")
                                            self.create_relationship(cb_node_id, bug_type_node_id, "BELONGS_TO_REQUIREMENT_TYPE")
                                            self.create_relationship(cb_node_id, feature_node_id, "ASSOCIATED_WITH_FEATURE")

class FileManager:
    """Handles file operations for reading JSON."""
    @staticmethod
    def read_json_from_file(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)


def Neo4jDataUploader(URI, AUTH, formatted_schema_dir=None):
    logger.info("__Neo4jDataUploader Sequence Initiated__")
    connection = Neo4jConnection()
    connection.connect(URI, AUTH)

    if connection.driver:
        processor = DataProcessor(connection.driver)

        for file_name in os.listdir(formatted_schema_dir):
            if file_name.endswith('.json') and file_name.startswith('Merged_'):
                json_file_path = os.path.join(formatted_schema_dir, file_name)
                data = FileManager.read_json_from_file(json_file_path)
                processor.process_data(data)
                logger.info(f"Data processing completed successfully for {file_name}.")

        logger.info("__Neo4jDataUploader Sequence Executed__\n")
        connection.close()
    else:
        logger.error("Failed to initialize database connection.\n")
