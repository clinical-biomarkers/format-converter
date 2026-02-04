import json
from collections import defaultdict

def extract_id_from_url(url):
    """Extract ID from URL and replace _ with :"""
    # Example: http://purl.obolibrary.org/obo/MONDO_0000001 -> MONDO:0000001
    parts = url.split('/')[-1]
    return parts.replace('_', ':')

def construct_omim_url(omim_id):
    """Construct OMIM URL from ID"""
    # Extract just the number part from OMIM:121210 or OMIMPS:121210
    number = omim_id.split(':')[-1]
    return f"https://www.omim.org/entry/{number}"

def process_mondo_json(input_file, output_file):
    # Load the input JSON
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Dictionary to store results, keyed by DOID
    disease_synonyms = defaultdict(list)
    
    # Process each node in the graph
    for graph in data.get('graphs', []):
        for node in graph.get('nodes', []):
            # Skip deprecated entries
            meta = node.get('meta', {})
            if meta.get('deprecated', False):
                continue
            
            # Check if this entry has a DOID xref
            xrefs = meta.get('xrefs', [])
            doid_list = [xref.get('val') for xref in xrefs if xref.get('val', '').startswith('DOID:')]
            
            if not doid_list:
                continue
            
            # For each DOID found, add synonyms
            for doid in doid_list:
                # Add MONDO entry as a synonym
                mondo_id = extract_id_from_url(node.get('id', ''))
                mondo_synonym = {
                    "id": mondo_id,
                    "name": node.get('lbl', ''),
                    "resource": "Mondo Disease Ontology",
                    "url": node.get('id', '')
                }
                disease_synonyms[doid].append(mondo_synonym)
                
                # Process synonyms to find OMIM entries with hasExactSynonym
                synonyms = meta.get('synonyms', [])
                for syn in synonyms:
                    # Only process hasExactSynonym entries
                    if syn.get('pred') != 'hasExactSynonym':
                        continue
                    
                    syn_xrefs = syn.get('xrefs', [])
                    for xref in syn_xrefs:
                        # Check if this is an OMIM or OMIMPS reference
                        if xref.startswith('OMIM:') or xref.startswith('OMIMPS:'):
                            # Create standardized OMIM ID (remove PS suffix for ID)
                            omim_id = xref.replace('OMIMPS:', 'OMIM:')
                            
                            omim_synonym = {
                                "id": omim_id,
                                "name": syn.get('val', ''),
                                "resource": "Online Mendelian Inheritance in Man",
                                "url": construct_omim_url(xref)
                            }
                            disease_synonyms[doid].append(omim_synonym)
    
    # Convert defaultdict to regular dict for JSON serialization
    output_data = dict(disease_synonyms)
    
    # Write output JSON
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"Processing complete. Found {len(output_data)} DOIDs with synonyms.")
    print(f"Output written to {output_file}")

if __name__ == "__main__":
    input_file = "/data/shared/repos/format-converter/mapping_data/mondo.json"
    output_file = "/data/shared/repos/format-converter/mapping_data/disease_syn.json"
    
    process_mondo_json(input_file, output_file)
