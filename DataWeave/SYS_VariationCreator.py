import json
import random
import os
from datetime import datetime

# Path to the folder containing input JSON files
input_folder = 'RawAppData/'
output_folder = 'ProcessedAppData/'


# Function to create ultra-simplified random combinations
def create_minimal_variations(data, num_combinations=100):
    variations = []
    unique_combinations = set()  # To track unique combinations

    # Extract the domain and subdomains
    domain = data['domain']
    subdomains = data['subdomains']

    while len(variations) < num_combinations:
        # Randomly select a subdomain
        subdomain = random.choice(subdomains)
        subsystems = subdomain['subsystems']

        # Select a random subsystem
        subsystem = random.choice(subsystems)
        technologies = subsystem['technologies']

        # Aggregate features, tools, standards, models, and technology names
        all_features = []
        all_associated_tools = set()
        all_standards_and_protocols = set()
        all_deployment_models = set()
        technology_names = {}
        involved_technologies = set()
        involved_subsystems = set()

        # Select 1-2 technologies from the available ones in the subsystem
        selected_technologies = random.sample(technologies, random.randint(1, 2))

        # For each selected technology, aggregate its features and other data
        for technology in selected_technologies:
            tech_name = technology['technology']
            features = technology['features']

            # Add the technology name to the involved technologies set
            involved_technologies.add(tech_name)
            involved_subsystems.add(subsystem['type'])  # Add the subsystem type to involved subsystems

            # Add features from the technology
            for feature in features:
                if feature['name'] not in technology_names:
                    technology_names[feature['name']] = []
                technology_names[feature['name']].append(tech_name)

                all_features.append(feature)

            # Collect associated tools, standards, and deployment models
            all_associated_tools.update(technology.get('associated_tools', []))
            all_standards_and_protocols.update(technology.get('standards_and_protocols', []))
            all_deployment_models.update(technology.get('deployment_models', []))

        # Randomly select 1-2 features from the aggregated features
        num_features_to_select = random.randint(1, 2)
        selected_features = random.sample(all_features, num_features_to_select)

        # Randomly select 1 item for tools, standards, and deployment models
        associated_tool = random.choice(list(all_associated_tools)) if all_associated_tools else None
        standard_and_protocol = random.choice(
            list(all_standards_and_protocols)) if all_standards_and_protocols else None
        deployment_model = random.choice(list(all_deployment_models)) if all_deployment_models else None

        # Aggregate characteristics for selected features
        def get_top_characteristics(key):
            characteristics = []
            for feature in selected_features:
                characteristics.extend(feature['characteristics'].get(key, []))
            # Deduplicate and limit to 2 items
            return list(dict.fromkeys(characteristics))[:2]

        # Create a minimalist variation
        variation = {
            "domain": domain,
            "subdomain": subdomain['name'],
            "feature_name": tuple([feature['name'] for feature in selected_features]),
            "performance": tuple(get_top_characteristics('performance')),
            "scalability": tuple(get_top_characteristics('scalability')),
            "latency": tuple(get_top_characteristics('latency')),
            "security": tuple(get_top_characteristics('security')),
            "compatibility": tuple(get_top_characteristics('compatibility')),
            "associated_tool": associated_tool,
            "standard_and_protocol": standard_and_protocol,
            "deployment_model": deployment_model,
            "technologies_involved": tuple([list(set(tech_names)) for feature, tech_names in technology_names.items() if
                                            feature in [f['name'] for f in selected_features]]),
            "subsystems_involved": tuple(involved_subsystems),
            "technologies_involved": tuple(involved_technologies),
            "date_created": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Create a unique identifier for the variation
        variation_id = tuple(variation.items())

        # Check if the variation is unique
        if variation_id not in unique_combinations:
            variations.append(variation)
            unique_combinations.add(variation_id)

    return variations


# Process all files in the RawAppData folder
def SYS_VariatioinCreator():
    for filename in os.listdir(input_folder):
        if filename.startswith("SYS_") and filename.endswith(".json"):
            input_path = os.path.join(input_folder, filename)

            # Load the OTT data from the JSON file
            with open(input_path, 'r') as file:
                ott_data = json.load(file)

            # Generate minimalist random combinations based on the loaded data
            domain_name = ott_data.get('domain', 'unknown')
            variations = create_minimal_variations(ott_data, num_combinations=500)

            # Save the variations to a new JSON file in the ProcessedAppData folder
            output_filename = f"VAR_{domain_name}_variations.json"
            output_path = os.path.join(output_folder, output_filename)

            with open(output_path, 'w') as outfile:
                json.dump(variations, outfile, indent=4)

            print(f"{len(variations)} variations have been created and saved to '{output_filename}'.")
