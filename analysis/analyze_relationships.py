import json
import logging
import os
from collections import defaultdict
from pathlib import Path

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    filename='/data/shared/repos/format-converter/logs/analyze_relationships.log',
    encoding='utf-8',
    format='%(levelname)s:%(message)s',
    level=logging.INFO
)

def analyze_biomarker_relationships(directory_path):
    """
    Analyze the relationship between assessed_biomarker_entity.recommended_name
    and assessed_biomarker_entity_id across all JSON files in a directory.

    Expected JSON structure:
    [
        {
            "biomarker_id": "AN4559-1",
            "biomarker_component": [
                {
                    "biomarker": "ABCB1 I1145I mutation",
                    "assessed_biomarker_entity": {
                        "recommended_name": "ABCB1"
                    },
                    "assessed_biomarker_entity_id": "NCBI:5243"
                }
            ]
        }
    ]
    """
    # Store relationships: id -> set of recommended_names
    id_to_names = defaultdict(set)
    # Store reverse mapping: name -> set of ids
    name_to_ids = defaultdict(set)

    # Track files processed and any issues
    files_processed = 0
    files_with_errors = []

    # Reset relationship details for each analysis
    if hasattr(process_biomarker_component, 'relationship_details'):
        delattr(process_biomarker_component, 'relationship_details')

    # Process all JSON files in the directory
    for file_path in Path(directory_path).glob("*.json"):
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            files_processed += 1
            logger.info(f"Processing: {file_path.name}")
            # Navigate through the JSON structure
            if isinstance(data, list):
                # Handle list of biomarker objects at top level
                extract_biomarker_data(data, id_to_names, name_to_ids, file_path.name)
            elif isinstance(data, dict):
                # Handle single biomarker object at top level
                extract_biomarker_data(data, id_to_names, name_to_ids, file_path.name)
        except json.JSONDecodeError as e:
            files_with_errors.append(f"{file_path.name}: JSON decode error - {e}")
        except Exception as e:
            files_with_errors.append(f"{file_path.name}: {e}")

    # Analyze relationships
    logger.info(f"\n{'='*60}")
    logger.info(f"Analysis summary")
    logger.info(f"\n{'='*60}")
    logger.info(f"Files processed: {files_processed}")
    logger.info(f"Files with errors: {len(files_with_errors)}")

    if files_with_errors:
        logger.warning("\nFiles with errors:")
        for error in files_with_errors:
            logger.warning(f" - {error}")

    logger.info(f"\nUnique biomarker entity IDs found: {len(id_to_names)}")
    logger.info(f"Unique recommended names found: {len(name_to_ids)}")

    # Check for inconsistencies
    logger.info(f"\n{'='*60}")
    logger.info("Relationship analysis")
    logger.info(f"\n{'='*60}")

    # Check if any ID maps to multiple names
    multiple_names = {id_val: names for id_val, names in id_to_names.items() if len(names) > 1}
    if multiple_names:
        logger.info(f"\n IDs with multiple names ({len(multiple_names)}):")
        for id_val, names in multiple_names.items():
            logger.info(f"ID '{id_val}' -> Names: {list(names)}")
    else:
        logger.info("\n No IDs map to multiple recommended names")

    # Check if any name maps to multiple IDs
    multiple_ids = {name: ids for name, ids in name_to_ids.items() if len(ids) > 1}
    if multiple_ids:
        logger.info(f"\n Names with multiple IDs ({len(multiple_ids)}):")
        for name, ids in multiple_ids.items():
            logger.info(f"Name '{name}' -> IDs: {list(ids)}")
    else:
        logger.info("\n No recommended names map to multiple IDs")

    # Show sample relationships with more detail
    logger.info(f"\n{'='*60}")
    logger.info("SAMPLE RELATIONSHIPS (first 10)")
    logger.info(f"{'='*60}")
    sample_count = 0
    for id_val, names in id_to_names.items():
        if sample_count >= 10:
            break
        name_list = list(names)
        logger.info(f"ID: {id_val} -> Name: {name_list[0] if name_list else 'None'}")
        sample_count += 1
    
    # Show detailed breakdown if there are relationship issues
    if hasattr(process_biomarker_component, 'relationship_details'):
        details = process_biomarker_component.relationship_details
        total_relationships = len(details)
        logger.info(f"\nTotal biomarker component relationships found: {total_relationships}")
        
        if multiple_names or multiple_ids:
            logger.info(f"\n{'='*60}")
            logger.info("DETAILED ISSUE BREAKDOWN")
            logger.info(f"{'='*60}")
            
            # Show examples of problematic relationships
            for id_val, names in list(multiple_names.items())[:5]:  # Show first 5
                logger.warning(f"\nID '{id_val}' has multiple names:")
                relevant_details = [d for d in details if d['entity_id'] == id_val]
                for detail in relevant_details[:3]:  # Show first 3 examples
                    logger.info(f"  - File: {detail['file']}, Biomarker: {detail['biomarker_name']}, Name: {detail['recommended_name']}")
            
            for name, ids in list(multiple_ids.items())[:5]:  # Show first 5
                logger.warning(f"\nName '{name}' has multiple IDs:")
                relevant_details = [d for d in details if d['recommended_name'] == name]
                for detail in relevant_details[:3]:  # Show first 3 examples
                    logger.info(f"  - File: {detail['file']}, Biomarker: {detail['biomarker_name']}, ID: {detail['entity_id']}")
    
    return {
        'id_to_names': dict(id_to_names),
        'name_to_ids': dict(name_to_ids),
        'files_processed': files_processed,
        'files_with_errors': files_with_errors,
        'multiple_names_per_id': multiple_names,
        'multiple_ids_per_name': multiple_ids,
        'total_relationships': len(getattr(process_biomarker_component, 'relationship_details', []))
    }

def extract_biomarker_data(data, id_to_names, name_to_ids, filename):
    """
    Extract biomarker data from the specific JSON structure.
    Expected structure: [{"biomarker_id": "...", "biomarker_component": [{"assessed_biomarker_entity": {"recommended_name": "..."}, "assessed_biomarker_entity_id": "..."}]}]
    """
    if isinstance(data, list):
        # Top level is a list of biomarker objects
        for biomarker_obj in data:
            if isinstance(biomarker_obj, dict) and 'biomarker_component' in biomarker_obj:
                biomarker_id = biomarker_obj.get('biomarker_id', 'Unknown')
                biomarker_components = biomarker_obj['biomarker_component']
                
                if isinstance(biomarker_components, list):
                    for component in biomarker_components:
                        process_biomarker_component(component, id_to_names, name_to_ids, filename, biomarker_id)
    
    elif isinstance(data, dict):
        # Handle case where data is a single biomarker object
        if 'biomarker_component' in data:
            biomarker_id = data.get('biomarker_id', 'Unknown')
            biomarker_components = data['biomarker_component']
            
            if isinstance(biomarker_components, list):
                for component in biomarker_components:
                    process_biomarker_component(component, id_to_names, name_to_ids, filename, biomarker_id)

def process_biomarker_component(component, id_to_names, name_to_ids, filename, biomarker_id):
    """
    Process a single biomarker component to extract the relationship.
    """
    if not isinstance(component, dict):
        return
    
    # Check if this component has the required fields
    if 'assessed_biomarker_entity' in component and 'assessed_biomarker_entity_id' in component:
        entity = component['assessed_biomarker_entity']
        entity_id = component['assessed_biomarker_entity_id']
        
        if isinstance(entity, dict) and 'recommended_name' in entity:
            recommended_name = entity['recommended_name']
            
            if entity_id is not None and recommended_name is not None:
                # Store the relationship with additional context
                id_str = str(entity_id)
                name_str = str(recommended_name)
                
                id_to_names[id_str].add(name_str)
                name_to_ids[name_str].add(id_str)
                
                # Store additional context for debugging
                if not hasattr(process_biomarker_component, 'relationship_details'):
                    process_biomarker_component.relationship_details = []
                
                process_biomarker_component.relationship_details.append({
                    'file': filename,
                    'biomarker_id': biomarker_id,
                    'biomarker_name': component.get('biomarker', 'Unknown'),
                    'entity_id': entity_id,
                    'recommended_name': recommended_name
                })

def main():
    """
    Main function to run the analysis.
    """
    # Get directory path from user
    directory = input("Enter the directory path containing JSON files: ").strip()
    
    if not os.path.exists(directory):
        logger.error(f"Error: Directory '{directory}' does not exist.")
        return
    
    if not os.path.isdir(directory):
        logger.error(f"Error: '{directory}' is not a directory.")
        return
    
    # Check if directory contains JSON files
    json_files = list(Path(directory).glob("*.json"))
    if not json_files:
        logger.warning(f"No JSON files found in directory '{directory}'.")
        return
    
    logger.info(f"Found {len(json_files)} JSON files in '{directory}'")
    
    # Run the analysis
    results = analyze_biomarker_relationships(directory)
    
    # Option to save results to a file
    save_results = input("\nWould you like to save the analysis results to a file? (y/n): ").strip().lower()
    if save_results == 'y':
        output_file = os.path.join(directory, "biomarker_relationship_analysis.json")
        with open(output_file, 'w', encoding='utf-8') as f:
            # Convert sets to lists for JSON serialization
            serializable_results = {}
            for key, value in results.items():
                if isinstance(value, dict):
                    serializable_results[key] = {k: list(v) if isinstance(v, set) else v 
                                               for k, v in value.items()}
                else:
                    serializable_results[key] = value
            
            json.dump(serializable_results, f, indent=2, ensure_ascii=False)
        logger.info(f"Results saved to: {output_file}")

if __name__ == "__main__":
    main()
