import os
import json

# Input and output folder paths
input_folder = "LLM_Processed_Files"
output_folder = "Formatted_Schema_Files"

# Ensure the output folder exists
os.makedirs(output_folder, exist_ok=True)

def transform_data(data):
    # Initialize the result structure
    transformed = {"domain": None, "subdomains": []}
    subdomain_map = {}

    for entry in data:
        domain = entry["metadata"]["domain"]
        subdomain_name = entry["metadata"]["subdomain"]
        subsystems = entry["metadata"]["subsystems_involved"]
        technologies = entry["metadata"]["technologies_involved"]

        # Set the domain at the root level
        if transformed["domain"] is None:
            transformed["domain"] = domain

        # Check if the subdomain already exists
        if subdomain_name not in subdomain_map:
            subdomain_map[subdomain_name] = {
                "name": subdomain_name,
                "subsystems": []
            }
            transformed["subdomains"].append(subdomain_map[subdomain_name])

        # Process each subsystem
        for subsystem in subsystems:
            subsystem_entry = {
                "type": subsystem,
                "technology": technologies,
                "features": entry["data"]["Features"],
                "user_stories": [],  # User stories directly under the subsystem
                "associated_tools": entry["data"].get("Associated Tool", []),
                "standards_and_protocols": entry["data"].get("Standard and Protocol", []),
                "deployment_models": entry["data"].get("Deployment Model", [])
            }

            # Add user stories
            for user_story in entry["data"]["User Stories"]:
                story_entry = {
                    "description": user_story["User Story"],
                    "quality": user_story["Quality"],
                    "contextual_characteristics": user_story.get("Contextual Characteristics", {}),
                    "common bugs":user_story.get("Common Bugs",{}),
                    "acceptance criteria": user_story.get("Acceptance Criteria",[])
                }
                subsystem_entry["user_stories"].append(story_entry)

            # Append subsystem to the subdomain
            subdomain_map[subdomain_name]["subsystems"].append(subsystem_entry)

    return transformed

def SYS_Matter2FormattedData():
    # Process all files starting with "VAR_" in the input folder
    for filename in os.listdir(input_folder):
        if filename.startswith("VAR_") and filename.endswith(".json"):
            input_path = os.path.join(input_folder, filename)
            output_filename = f"FOR_{filename}"
            output_path = os.path.join(output_folder, output_filename)

            # Read and transform the data
            with open(input_path, "r") as infile:
                raw_data = json.load(infile)

            formatted_data = transform_data(raw_data)

            # Write the transformed data to the output file
            with open(output_path, "w") as outfile:
                json.dump(formatted_data, outfile, indent=4)

            print(f"Processed: {filename} -> {output_filename}")

    print(f"All files have been processed and saved to {output_folder}")
