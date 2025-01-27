import json
import re
import logging
import sys
from groq import Groq
import os
from DataWeave.SYS_VariationCreator import SYS_VariatioinCreator

# Initialize logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Groq client
api_key = "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"
client = Groq(api_key=api_key)


STATE_FILE_PATH = "SYS_global_state.json"
def reset_global_state():
    global global_state
    global_state = {
        "current_file": None,
        "file_paths": {},
        "models": [
            "llama-3.3-70b-versatile",
            "llama-3.2-90b-vision-preview",
            "gemma2-9b-it"
        ],
        "last_model_index": 0,
        "Total_Batches": global_state["Total_Batches"],
        "current_model": "llama-3.3-70b-versatile",
        "exhausted_models": [],
        "processed_files": [],
    }
if os.path.exists(STATE_FILE_PATH):
    try:
        with open(STATE_FILE_PATH, "r") as state_file:
            global_state = json.load(state_file)
            # Convert 'processed_files' to a set for faster lookup
            # Ensure 'file_paths' exists for the new structure
            if "file_paths" not in global_state:
                global_state["file_paths"] = {}
        logging.info("Global state loaded successfully.")
    except (json.JSONDecodeError, IOError) as e:
        logging.info(f"Error loading global state: {e}. Reinitializing state.")
        global_state = {
            "current_file": None,
            "file_paths": {},  # Initialize with an empty dictionary for file paths
            "models": [
                "llama-3.3-70b-versatile",
                "llama-3.2-90b-vision-preview",
                "gemma2-9b-it"
            ],
            "last_model_index": 0,
            "Total_Batches": 0,
            "current_model": None,
            "exhausted_models": [],
            "processed_files": []
        }
else:
    global_state = {
        "current_file": None,
        "file_paths": {},  # Initialize with an empty dictionary for file paths
        "models": [
            "llama-3.3-70b-versatile",
            "llama-3.2-90b-vision-preview",
            "gemma2-9b-it"
        ],
        "last_model_index": 0,
        "Total_Batches": 0,
        "current_model": None,
        "exhausted_models": [],
        "processed_files": []
    }
    logging.info("Global state file not found. Initialized with default values.")
global_state["current_model"] = global_state["models"][global_state["last_model_index"]]

def save_state():
    global global_state
    state_to_save = global_state.copy()
    state_to_save["processed_files"] = list(global_state["processed_files"])  # Convert set to list for serialization
    with open(STATE_FILE_PATH, "w") as state_file:
        json.dump(state_to_save, state_file, indent=4)
def switch_model():
    global global_state
    available_models = [m for m in global_state["models"] if m not in global_state["exhausted_models"]]

    if available_models:
        global_state["last_model_index"] = (global_state["last_model_index"] + 1) % len(global_state["models"])
        global_state["current_model"] = global_state["models"][global_state["last_model_index"]]
        logging.info(f"Switched to model: {global_state['current_model']}")
    else:
        logging.warning("All models are exhausted. No model to switch to.")
        global_state["current_model"] = None

    save_state()

# Handle rate limits and errors
def handle_rate_limits(error_message=None):
    global global_state

    if error_message and "429" in str(error_message):
        logging.warning(f"Rate limit reached for {global_state['current_model']}.")
        if global_state["current_model"] not in global_state["exhausted_models"]:
            global_state["exhausted_models"].append(global_state["current_model"])
        switch_model()

# Check if all models are exhausted
def all_models_exhausted():
    global global_state
    return len(global_state["exhausted_models"]) == len(global_state["models"])

# Step 1: Extract components
def extract_components(component_list):
    extracted_components = []
    for component in component_list:
        extracted_components.append({
            "domain": component.get("domain", ""),
            "subdomain": component.get("subdomain", ""),
            "feature_name": component.get("feature_name", []),
            "performance": component.get("performance", []),
            "scalability": component.get("scalability", []),
            "latency": component.get("latency", []),
            "security": component.get("security", []),
            "compatibility": component.get("compatibility", []),
            "associated_tool": component.get("associated_tool", ""),
            "standard_and_protocol": component.get("standard_and_protocol", ""),
            "deployment_model": component.get("deployment_model", ""),
            "technologies_involved": component.get("technologies_involved", []),
            "subsystems_involved": component.get("subsystems_involved", [])
        })
    return extracted_components

# Step 2: Generate user story prompt
def generate_user_story_for_all_qualities(domain, subdomain, feature_name, performance, scalability, latency, security, compatibility, associated_tool, standard_and_protocol, deployment_model, technologies_involved, subsystems_involved):
    example_json = """
        ```json
        {
            "Features": [
                "Automated Decision Support",
                "Real-Time Weather Data Integration"
            ],
            "User Stories": [
                {
                    "Quality": "High",
                    "User Story": "As a user, I want Automated Decision Support and Real-Time Weather Data Integration so that I can make fast and reliable decisions in real-time scenarios.",
                    "Acceptance Criteria": ["Ensure fast and reliable decision-making", "Optimize for real-time operations"],
                    "Common Bugs": {
                        "FR": ["BUG1", "Bug2"],
                        "NFR": ["BUG1", "Bug2"]
                    },
                    "Contextual Characteristics": {
                        "Performance": ["Fast and reliable decision-making", "Optimized for real-time operations"],
                        "Scalability": ["Handling complex decision scenarios", "Support for multiple stakeholders"],
                        "Latency": ["Instant decision support", "Minimized downtime for decision-making"],
                        "Security": ["Protection of sensitive decision-making data", "Compliance with regulatory requirements"],
                        "Compatibility": ["Integration with existing decision support systems", "Support for multiple decision-making models"]
                    }
                },
                {
                    "Quality": "Average",
                    "User Story": "As a user, I want Automated Decision Support for improved decision-making capabilities.",
                    "Acceptance Criteria": ["Ensure fast decision-making", "Handle moderate complexity"],
                    "Common Bugs": {
                        "FR": ["BUG1", "Bug2"],
                        "NFR": ["BUG1", "Bug2"]
                    },
                    "Contextual Characteristics": {
                        "Performance": ["Reliable decision-making"],
                        "Scalability": ["Support for multiple stakeholders"],
                        "Latency": ["Quick response times"],
                        "Security": ["Protection of sensitive data"],
                        "Compatibility": ["Basic integration with decision support systems"]
                    }
                },
                {
                    "Quality": "Low",
                    "User Story": "As a user, I want Real-Time Weather Data Integration.",
                    "Acceptance Criteria": ["Basic weather data integration"],
                    "Common Bugs": {
                        "FR": ["BUG1", "Bug2"],
                        "NFR": ["BUG1", "Bug2"]
                    },
                    "Contextual Characteristics": {
                        "Performance": ["Basic functionality"],
                        "Scalability": ["Limited to single stakeholder"],
                        "Latency": ["Moderate response times"],
                        "Security": ["Basic protection measures"],
                        "Compatibility": ["Minimal integration"]
                    }
                }
            ],
            "Associated Tool": "Google Analytics",
            "Standard and Protocol": "HTTP/HTTPS",
            "Deployment Model": "Cloud (AWS, Azure, Google Cloud, etc.)"
        }
        ```
        """

    prompt = f"""
        Context:
        - **Domain**: {domain}
        - **Subdomain**: {subdomain}
        - **Features**: {', '.join(feature_name)}

        Key Metrics:
        - **Performance**: {', '.join(performance)}
        - **Scalability**: {', '.join(scalability)}
        - **Latency**: {', '.join(latency)}
        - **Security**: {', '.join(security)}
        - **Compatibility**: {', '.join(compatibility)}

        Additional Information:
        - **Associated Tool**: {associated_tool}
        - **Standards and Protocols**: {standard_and_protocol}
        - **Deployment Model**: {deployment_model}
        - **Technologies Involved**: {', '.join(technologies_involved)}
        - **Subsystems Involved**: {', '.join(subsystems_involved)}

        Objective:
        Generate user stories of varying quality levels (High, Average, Low) based on the provided context. Adhere to these guidelines:
        - Base Story: Write a coherent and contextually meaningful user story.
        - High-Quality Story: Add depth, complexity, and detail, including specific contextual characteristics and multiple acceptance criteria.
        - Average-Quality Story: Focus on core ideas but reduce complexity and context.
        - Low-Quality Story: Simplify to a minimal level with vague or incomplete details.

        Notes:
        - Understand the relationship between the given data points (e.g., domain, subdomain, features, and metrics) before proceeding with user story generation.
        - Do not use the exact names from the input directly. Instead, capture the **semantic meaning** of the parameters to ensure that the generated user story is contextually coherent and not just a combination of the inputs.
        - Ensure the stories reflect the influence of the context and characteristics provided and are semantically correct.
        - The output must strictly adhere to the JSON format as shown in the following example:
        {example_json}
    """

    return prompt

# Step 3: Parse user story output
def parse_user_story_output(user_story):
    try:
        # Extract JSON block
        json_block_pattern = r"```json\s*(.*?)\s*```"
        json_match = re.search(json_block_pattern, user_story, re.DOTALL)

        if json_match:
            json_data = json_match.group(1).strip()
            parsed_json = json.loads(json_data)
        elif user_story.strip().startswith("{") and user_story.strip().endswith("}"):
            parsed_json = json.loads(user_story.strip())
        else:
            logging.warning("No valid JSON block found, using fallback parsing.")

        metadata = {
            "associated_tool": parsed_json.get("Associated Tool", ""),
            "standard_and_protocol": parsed_json.get("Standard and Protocol", ""),
            "deployment_model": parsed_json.get("Deployment Model", "")
        }

        return {
            "metadata": metadata,
            "data": parsed_json
        }

    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON: {e}")
        return {"error": "Invalid JSON format"}
    except Exception as e:
        logging.error(f"Unexpected error during parsing: {e}")
        return {"error": str(e)}

# Step 4: Generate JSON output and save to file
def create_json_output(components, parsed_results):
    final_json = []
    for component, result in zip(components, parsed_results):
        if "error" in result:
            logging.warning(f"Error in processing component {component}: {result['error']}")
            result["data"] = {
                "Features": [],
                "User Stories": [],
                "Associated Tool": "",
                "Standard and Protocol": "",
                "Deployment Model": ""
            }

        final_json.append({
            "metadata": {
                "domain": component["domain"],
                "subdomain": component["subdomain"],
                "subsystems_involved": component["subsystems_involved"],
                "technologies_involved": component["technologies_involved"]
            },
            "data": result.get("data", {})
        })


    return final_json

# Step 5: Call the LLM with retries
def call_llm(prompt, retries=3):
    for attempt in range(retries):
        if all_models_exhausted():
            global_state['exhausted_models'] = []
            global_state["current_model"] = "llama-3.3-70b-versatile"
            global_state['last_model_index'] = 0
            logging.error("All models have exhausted their rate limits.")
            save_state()
            sys.exit(0)

        try:
            completion = client.chat.completions.create(
                model=global_state["current_model"],
                messages=[{"role": "system", "content": prompt}],
                temperature=1,
                max_completion_tokens=1024,
                top_p=1,
                stream=False,
                stop=None
            )
            user_story = completion.choices[0].message.content
            logging.info(f"LLM Response: {user_story}")
            return user_story

        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            handle_rate_limits(str(e))
            if attempt == retries - 1:
                return {"error": "LLM call failed after multiple attempts"}

# Load the data from the JSON file
def load_variation_data(file_path):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
        return data
    except Exception as e:
        logging.error(f"Error loading JSON file: {e}")
        return {"error": str(e)}

# Extract the first 3 components
def extract_threshold(variation_data):
    if isinstance(variation_data, list):
        return variation_data[:5]
    else:
        logging.error("The data is not in expected list format.")
        return []

def ensure_output_folder_exists(output_folder_path):
    """
    Ensure the output folder exists. Create it if it doesn't exist.
    """
    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)
        logging.info(f"Created output folder: {output_folder_path}")


def save_output_file(output_folder_path, domain_name, parsed_results):
    """
    Save parsed results to an output file in the specified folder.

    Args:
        output_folder_path (str): The folder where the output file should be saved.
        domain_name (str): The domain name extracted from the input file name.
        parsed_results (list): The list of parsed results to save.
    """
    # Update the output file name format
    output_file_name = f"VAR_{domain_name}_variations.json"
    output_file_path = os.path.join(output_folder_path, output_file_name)

    try:
        with open(output_file_path, "w") as output_file:
            json.dump(parsed_results, output_file, indent=4)
        logging.info(f"Saved output file: {output_file_path}")
    except IOError as e:
        logging.error(f"Failed to save output file: {e}")


def pipeline(input_file_paths, file_to_domain_map, output_folder_path, batch_size=4):
    global global_state

    # Ensure the output folder exists
    ensure_output_folder_exists(output_folder_path)

    # Initialize global state keys if not already initialized
    if "file_paths" not in global_state:
        global_state["file_paths"] = {}
    if "processed_files" not in global_state:
        global_state["processed_files"] = []
    if "current_file" not in global_state:
        global_state["current_file"] = None

    all_parsed_results = []

    # Filter out processed files and calculate the remaining batch size
    remaining_files = [file for file in input_file_paths if os.path.basename(file) not in global_state["processed_files"]]
    num_remaining_files = len(remaining_files)

    if num_remaining_files == 0:
        logging.info("All input files have already been processed. Exiting pipeline.")
        return

    # Recalculate batch distribution only for unprocessed files
    batch_per_file = batch_size // num_remaining_files
    remainder = batch_size % num_remaining_files

    # Allocate components per file (extra components go to the first `remainder` files)
    file_batches = {file: batch_per_file + (1 if idx < remainder else 0)
                    for idx, file in enumerate(remaining_files)}

    logging.info(f"Batch distribution across unprocessed files: {file_batches}")

    for file_path in input_file_paths:
        file_name = os.path.basename(file_path)

        # Skip files that are already fully processed
        if file_name in global_state["processed_files"]:
            logging.info(f"File '{file_name}' is already processed. Skipping.")
            continue

        # Ensure the file is initialized in global_state["file_paths"]
        if file_name not in global_state["file_paths"]:
            global_state["file_paths"][file_name] = 0  # Initialize starting index to 0

        # Load variation data for the current file
        variation_data = load_variation_data(file_path)
        if "error" in variation_data:
            logging.error(f"Error loading data for file '{file_name}': {variation_data['error']}")
            continue

        # Extract the Threshold Variation components
        current_components = variation_data

        if not current_components:
            logging.warning(f"No components found for file: {file_name}")
            continue

        # Get the start and end index for batch processing
        start_index = global_state["file_paths"][file_name]
        end_index = min(start_index + file_batches[file_path], len(current_components))

        # Process components within the batch
        parsed_results = []  # Store parsed results for the current file
        for i in range(start_index, end_index):
            component = current_components[i]

            # Generate prompt and call the model
            prompt = generate_user_story_for_all_qualities(**component)
            try:
                user_story = call_llm(prompt)
            except Exception as e:
                handle_rate_limits(error_message=e)
                if all_models_exhausted():
                    global_state['exhausted_models'] = []
                    global_state["current_model"] = "llama-3.3-70b-versatile"
                    global_state['last_model_index'] = 0
                    logging.info("All models exhausted, stopping pipeline.")
                    save_state()
                    return

            # Check for errors in user story output
            if "error" in user_story:
                continue

            # Parse the result and append to the results
            parsed_result = parse_user_story_output(user_story)
            parsed_results.append(parsed_result)

            # Update the index for the current file
            global_state["file_paths"][file_name] = i + 1
            global_state["current_file"] = file_name
            save_state()
            logging.info(f"Processing file: {file_name}, component index: {i + 1}")

        # Create JSON output for the current file
        file_json_output = create_json_output(current_components[start_index:end_index], parsed_results)

        # If all components in the file are processed, mark the file as completed
        if global_state["file_paths"][file_name] >= len(current_components):
            global_state["processed_files"].append(file_name)
            logging.info(f"File '{file_name}' fully processed and added to 'processed_files'.")

        # Save output file for the current file
        domain_name = file_to_domain_map.get(file_name, "unknown")
        save_output_file(output_folder_path, domain_name, file_json_output)

        # Save state after processing each file
        save_state()

    logging.info("Pipeline execution complete.")
    if len(global_state["processed_files"]) == len(input_file_paths):
        logging.info("All files processed. Resetting global state.")
        reset_global_state()  # Reset global state for future runs
        SYS_VariatioinCreator()
        save_state()


def SYS_UserStoryGenration():
    input_file_paths = [
        os.path.join("ProcessedAppData", f)
        for f in os.listdir("ProcessedAppData")
        if os.path.isfile(os.path.join("ProcessedAppData", f)) and f.startswith("VAR_") and f.endswith(".json")
    ]
    file_to_domain_map = {
        os.path.basename(f): re.search(r"VAR_(.*?)_variations\.json", f).group(1)
        for f in input_file_paths
    }
    output_folder_path = "LLM_Processed_Files"

    pipeline(input_file_paths, file_to_domain_map, output_folder_path)
