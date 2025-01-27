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
        query = f"MERGE (n:{label} {{ {properties} }}) RETURN id(n) AS node_id"
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
        # Process Domain
        domain_name = data["domain"]
        domain_properties = f'domain: "{domain_name}"'
        domain_node_id = self.create_or_find_node("Domain", domain_properties)

        for subdomain in data["subdomains"]:
            # Process Subdomain
            subdomain_name = subdomain["name"]
            subdomain_properties = f'subdomain_name: "{subdomain_name}"'
            subdomain_node_id = self.create_or_find_node("Subdomain", subdomain_properties)
            self.create_relationship(subdomain_node_id, domain_node_id, "BELONGS_TO_DOMAIN")

            for subsystem in subdomain["subsystems"]:
                # Process Subsystem
                subsystem_type = subsystem["type"]
                subsystem_properties = f'subsystem_type: "{subsystem_type}"'
                subsystem_node_id = self.create_or_find_node("Subsystem", subsystem_properties)
                self.create_relationship(subsystem_node_id, subdomain_node_id, "BELONGS_TO_SUBDOMAIN")

                # Add Technology Connections to Subsystem
                for technology in subsystem["technology"]:
                    technology_properties = f'technology: "{technology}"'
                    technology_node_id = self.create_or_find_node("Technology", technology_properties)
                    self.create_relationship(technology_node_id, subsystem_node_id, "RELATED_TO_SUBSYSTEM")
                    self.create_relationship(technology_node_id, subdomain_node_id, "UTILIZED_IN_SUBDOMAIN")

                # Add Feature Connections to Subsystem
                for feature in subsystem["features"]:
                    feature_properties = f'feature: "{feature}"'
                    feature_node_id = self.create_or_find_node("Feature", feature_properties)
                    self.create_relationship(feature_node_id, subsystem_node_id, "HAS_FEATURE")
                    self.create_relationship(feature_node_id, subdomain_node_id, "IS_A_FEATURE_OF")

                # Add User Story Connections
                for user_story in subsystem["user_stories"]:
                    user_story_text = user_story["description"]
                    user_quality = user_story["quality"]
                    user_story_properties = f'user_story: "{user_story_text}", quality: "{user_quality}"'
                    user_story_node_id = self.create_or_find_node("UserStory", user_story_properties)
                    self.create_relationship(user_story_node_id, subsystem_node_id, "ASSOCIATED_WITH_SUBSYSTEM")
                    self.create_relationship(user_story_node_id, feature_node_id, "IS_A_USER_STORY_FOR")

                    # Connect User Story to Subdomain
                    self.create_relationship(user_story_node_id, subdomain_node_id, "ASSOCIATED_WITH_SUBDOMAIN")

                    # Process Acceptance Criteria
                    for acceptance_criteria in user_story["acceptance criteria"]:
                        ac_properties = f'acceptance_criteria: "{acceptance_criteria}"'
                        ac_node_id = self.create_or_find_node("AcceptanceCriteria", ac_properties)
                        self.create_relationship(ac_node_id, user_story_node_id, "IS_CRITERIA_FOR")

                    # Process Common Bugs
                    common_bugs = user_story["common bugs"]
                    for bug_type, bugs in common_bugs.items():
                        # Replace FR and NFR with Functional and Non-Functional
                        if bug_type == "FR":
                            bug_type = "functional"
                        elif bug_type == "NFR":
                            bug_type = "non-functional"

                        bug_type_properties = f'bug_type: "{bug_type}"'
                        bug_type_node_id = self.create_or_find_node("RequirementType", bug_type_properties)
                        self.create_relationship(bug_type_node_id, user_story_node_id, "HAS_BUG_TYPE")

                        for bug in bugs:
                            bug_properties = f'commonbugs: "{bug}"'
                            bug_node_id = self.create_or_find_node("CommonBug", bug_properties)
                            self.create_relationship(bug_node_id, bug_type_node_id, "IS_OF_BUG_TYPE")
                            self.create_relationship(bug_node_id, user_story_node_id, "REPORTED_IN_USER_STORY")

                    # Process Contextual Characteristics
                    for context_type, context_values in user_story["contextual_characteristics"].items():
                        for context_value in context_values:
                            context_node_type = f"{context_type}Characteristic"
                            context_properties = f'context_value: "{context_value}"'
                            context_node_id = self.create_or_find_node(context_node_type, context_properties)
                            self.create_relationship(context_node_id, user_story_node_id, f"HAS_{context_type.upper()}")
                            self.create_relationship(context_node_id, subsystem_node_id, "INFLUENCES_SUBSYSTEM")

                # Process Tools, Standards, and Deployment Models for Subsystem
                if "associated_tools" in subsystem:
                    tools_properties = f'tool: "{subsystem["associated_tools"]}"'
                    tools_node_id = self.create_or_find_node("AssociatedTool", tools_properties)
                    self.create_relationship(tools_node_id, subsystem_node_id, "USES_TOOL")

                if "standards_and_protocols" in subsystem:
                    standards_properties = f'standard: "{subsystem["standards_and_protocols"]}"'
                    standards_node_id = self.create_or_find_node("StandardOrProtocol", standards_properties)
                    self.create_relationship(standards_node_id, subsystem_node_id, "FOLLOWS_STANDARD")
                    self.create_relationship(standards_node_id, subdomain_node_id, "ADHERES_TO_STANDARD")

                if "deployment_models" in subsystem:
                    deployment_properties = f'deployment_model: "{subsystem["deployment_models"]}"'
                    deployment_node_id = self.create_or_find_node("DeploymentModel", deployment_properties)
                    self.create_relationship(deployment_node_id, subsystem_node_id, "DEPLOYED_AS")
                    self.create_relationship(deployment_node_id, subdomain_node_id, "USED_IN_SUBDOMAIN")

class FileManager:
    """Handles file operations for reading JSON."""
    @staticmethod
    def read_json_from_file(file_path):
        with open(file_path, 'r') as file:
            return json.load(file)

def SYS_Neo4jDataUploader(URI, AUTH, formatted_schema_dir=None):
    logger.info("__Neo4jDataUploader Sequence Initiated__")
    connection = Neo4jConnection()
    connection.connect(URI, AUTH)

    if connection.driver:
        processor = DataProcessor(connection.driver)

        # Filter files that start with 'FOR_VAR' and have a '.json' extension
        for file_name in os.listdir(formatted_schema_dir):
            if file_name.startswith('FOR_VAR') and file_name.endswith('.json'):
                json_file_path = os.path.join(formatted_schema_dir, file_name)
                try:
                    data = FileManager.read_json_from_file(json_file_path)
                    processor.process_data(data)
                    logger.info(f"Data processing completed successfully for {file_name}.")
                except Exception as e:
                    logger.error(f"An error occurred while processing {file_name}: {e}")
        logger.info("__Neo4jDataUploader Sequence Executed__\n")
        connection.close()
    else:
        logger.error("Failed to initialize database connection.\n")
