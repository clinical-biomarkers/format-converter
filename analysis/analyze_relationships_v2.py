import json
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, Set, List, Tuple, Any
import glob

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/data/shared/repos/format-converter/logs/analyze_relationships_v2.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class RelationshipAnalyzer:
    def __init__(self):
        # Track relationships: name -> set of IDs
        self.biomarker_name_to_ids: Dict[str, Set[str]] = defaultdict(set)
        self.biomarker_id_to_names: Dict[str, Set[str]] = defaultdict(set)
        
        self.condition_name_to_ids: Dict[str, Set[str]] = defaultdict(set)
        self.condition_id_to_names: Dict[str, Set[str]] = defaultdict(set)
        
        self.exposure_name_to_ids: Dict[str, Set[str]] = defaultdict(set)
        self.exposure_id_to_names: Dict[str, Set[str]] = defaultdict(set)
        
        self.total_records = 0
        self.processed_files = 0

    def process_file(self, file_path: str) -> None:
        """Process a single JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                logger.warning(f"File {file_path} does not contain an array at root level")
                return
                
            logger.info(f"Processing {file_path} with {len(data)} records")
            
            for record in data:
                self.process_record(record)
                self.total_records += 1
            
            self.processed_files += 1
            logger.info(f"Completed processing {file_path}")
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {file_path}: {e}")
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")

    def process_record(self, record: Dict[str, Any]) -> None:
        """Process a single record and extract relationships."""
        
        # Process biomarker components
        if 'biomarker_component' in record:
            for component in record['biomarker_component']:
                if 'assessed_biomarker_entity' in component and 'assessed_biomarker_entity_id' in component:
                    entity = component['assessed_biomarker_entity']
                    entity_id = component['assessed_biomarker_entity_id']
                    
                    if 'recommended_name' in entity and entity['recommended_name']:
                        name = entity['recommended_name']
                        self.biomarker_name_to_ids[name].add(entity_id)
                        self.biomarker_id_to_names[entity_id].add(name)
        
        # Process condition
        if 'condition' in record:
            condition = record['condition']
            if 'id' in condition and 'recommended_name' in condition:
                condition_id = condition['id']
                if 'name' in condition['recommended_name']:
                    condition_name = condition['recommended_name']['name']
                    self.condition_name_to_ids[condition_name].add(condition_id)
                    self.condition_id_to_names[condition_id].add(condition_name)
        
        # Process exposure agent
        if 'exposure_agent' in record:
            exposure = record['exposure_agent']
            if 'id' in exposure and 'recommended_name' in exposure:
                exposure_id = exposure['id']
                if 'name' in exposure['recommended_name']:
                    exposure_name = exposure['recommended_name']['name']
                    self.exposure_name_to_ids[exposure_name].add(exposure_id)
                    self.exposure_id_to_names[exposure_id].add(exposure_name)

    def analyze_relationships(self) -> None:
        """Analyze and report on one-to-one relationships."""
        logger.info("="*60)
        logger.info("RELATIONSHIP ANALYSIS RESULTS")
        logger.info("="*60)
        logger.info(f"Total records processed: {self.total_records}")
        logger.info(f"Total files processed: {self.processed_files}")
        logger.info("")
        
        # Analyze biomarker relationships
        self._analyze_biomarker_relationships()
        
        # Analyze condition relationships
        self._analyze_condition_relationships()
        
        # Analyze exposure agent relationships
        self._analyze_exposure_relationships()

    def _analyze_biomarker_relationships(self) -> None:
        """Analyze biomarker entity name to ID relationships."""
        logger.info("BIOMARKER ENTITY ANALYSIS")
        logger.info("-" * 40)
        
        total_names = len(self.biomarker_name_to_ids)
        total_ids = len(self.biomarker_id_to_names)
        
        logger.info(f"Unique biomarker names: {total_names}")
        logger.info(f"Unique biomarker IDs: {total_ids}")
        
        # Check name -> ID mapping
        names_with_multiple_ids = {name: ids for name, ids in self.biomarker_name_to_ids.items() if len(ids) > 1}
        logger.info(f"Names mapping to multiple IDs: {len(names_with_multiple_ids)}")
        
        # Check ID -> name mapping
        ids_with_multiple_names = {id_: names for id_, names in self.biomarker_id_to_names.items() if len(names) > 1}
        logger.info(f"IDs mapping to multiple names: {len(ids_with_multiple_names)}")
        
        # Report violations
        if names_with_multiple_ids:
            logger.warning("Names with multiple IDs (violates one-to-one):")
            for name, ids in list(names_with_multiple_ids.items())[:10]:  # Show first 10
                logger.warning(f"  '{name}' -> {list(ids)}")
            if len(names_with_multiple_ids) > 10:
                logger.warning(f"  ... and {len(names_with_multiple_ids) - 10} more")
        
        if ids_with_multiple_names:
            logger.warning("IDs with multiple names (violates one-to-one):")
            for id_, names in list(ids_with_multiple_names.items())[:10]:  # Show first 10
                logger.warning(f"  '{id_}' -> {list(names)}")
            if len(ids_with_multiple_names) > 10:
                logger.warning(f"  ... and {len(ids_with_multiple_names) - 10} more")
        
        is_one_to_one = len(names_with_multiple_ids) == 0 and len(ids_with_multiple_names) == 0
        logger.info(f"Biomarker name-ID relationship is one-to-one: {is_one_to_one}")
        logger.info("")

    def _analyze_condition_relationships(self) -> None:
        """Analyze condition name to ID relationships."""
        logger.info("CONDITION ANALYSIS")
        logger.info("-" * 40)
        
        total_names = len(self.condition_name_to_ids)
        total_ids = len(self.condition_id_to_names)
        
        logger.info(f"Unique condition names: {total_names}")
        logger.info(f"Unique condition IDs: {total_ids}")
        
        # Check name -> ID mapping
        names_with_multiple_ids = {name: ids for name, ids in self.condition_name_to_ids.items() if len(ids) > 1}
        logger.info(f"Names mapping to multiple IDs: {len(names_with_multiple_ids)}")
        
        # Check ID -> name mapping
        ids_with_multiple_names = {id_: names for id_, names in self.condition_id_to_names.items() if len(names) > 1}
        logger.info(f"IDs mapping to multiple names: {len(ids_with_multiple_names)}")
        
        # Report violations
        if names_with_multiple_ids:
            logger.warning("Condition names with multiple IDs (violates one-to-one):")
            for name, ids in list(names_with_multiple_ids.items())[:10]:
                logger.warning(f"  '{name}' -> {list(ids)}")
            if len(names_with_multiple_ids) > 10:
                logger.warning(f"  ... and {len(names_with_multiple_ids) - 10} more")
        
        if ids_with_multiple_names:
            logger.warning("Condition IDs with multiple names (violates one-to-one):")
            for id_, names in list(ids_with_multiple_names.items())[:10]:
                logger.warning(f"  '{id_}' -> {list(names)}")
            if len(ids_with_multiple_names) > 10:
                logger.warning(f"  ... and {len(ids_with_multiple_names) - 10} more")
        
        is_one_to_one = len(names_with_multiple_ids) == 0 and len(ids_with_multiple_names) == 0
        logger.info(f"Condition name-ID relationship is one-to-one: {is_one_to_one}")
        logger.info("")

    def _analyze_exposure_relationships(self) -> None:
        """Analyze exposure agent name to ID relationships."""
        logger.info("EXPOSURE AGENT ANALYSIS")
        logger.info("-" * 40)
        
        total_names = len(self.exposure_name_to_ids)
        total_ids = len(self.exposure_id_to_names)
        
        logger.info(f"Unique exposure agent names: {total_names}")
        logger.info(f"Unique exposure agent IDs: {total_ids}")
        
        # Check name -> ID mapping
        names_with_multiple_ids = {name: ids for name, ids in self.exposure_name_to_ids.items() if len(ids) > 1}
        logger.info(f"Names mapping to multiple IDs: {len(names_with_multiple_ids)}")
        
        # Check ID -> name mapping
        ids_with_multiple_names = {id_: names for id_, names in self.exposure_id_to_names.items() if len(names) > 1}
        logger.info(f"IDs mapping to multiple names: {len(ids_with_multiple_names)}")
        
        # Report violations
        if names_with_multiple_ids:
            logger.warning("Exposure agent names with multiple IDs (violates one-to-one):")
            for name, ids in list(names_with_multiple_ids.items())[:10]:
                logger.warning(f"  '{name}' -> {list(ids)}")
            if len(names_with_multiple_ids) > 10:
                logger.warning(f"  ... and {len(names_with_multiple_ids) - 10} more")
        
        if ids_with_multiple_names:
            logger.warning("Exposure agent IDs with multiple names (violates one-to-one):")
            for id_, names in list(ids_with_multiple_names.items())[:10]:
                logger.warning(f"  '{id_}' -> {list(names)}")
            if len(ids_with_multiple_names) > 10:
                logger.warning(f"  ... and {len(ids_with_multiple_names) - 10} more")
        
        is_one_to_one = len(names_with_multiple_ids) == 0 and len(ids_with_multiple_names) == 0
        logger.info(f"Exposure agent name-ID relationship is one-to-one: {is_one_to_one}")
        logger.info("")

def main():
    """Main function to process JSON files and analyze relationships."""
    
    # Get JSON files - adjust the pattern as needed
    json_files = glob.glob("*.json")  # All JSON files in current directory
    # Or specify a pattern like: json_files = glob.glob("data/*.json")
    
    if not json_files:
        logger.error("No JSON files found. Please check the file pattern.")
        return
    
    logger.info(f"Found {len(json_files)} JSON files to process")
    
    analyzer = RelationshipAnalyzer()
    
    # Process all files
    for file_path in json_files:
        analyzer.process_file(file_path)
    
    # Analyze relationships
    analyzer.analyze_relationships()
    
    logger.info("Analysis complete. Results saved to 'biomarker_analysis.log'")

if __name__ == "__main__":
    main()
