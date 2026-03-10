import os
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional

import sys
# Add project root to sys.path for absolute imports when running as script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.models.schemas import ModuleNode, DatasetNode

def parse_yaml_file(file_path: str) -> Dict[str, Any]:
    """
    Safely load a YAML file and return its content as a dictionary.
    Handles potential parsing errors gracefully.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = yaml.safe_load(f)
            return content if isinstance(content, dict) else {}
    except Exception as e:
        print(f"[WARNING] Could not parse YAML file {file_path}: {e}")
        return {}

def extract_nodes_from_schema_yml(file_path: str) -> List[DatasetNode]:
    """
    Parses a dbt schema.yml file to extract DatasetNodes.
    Extracts model names, descriptions, and column details.
    """
    nodes = []
    content = parse_yaml_file(file_path)
    
    # Get relative path for traceability
    try:
        rel_path = str(Path(file_path).relative_to(Path.cwd()))
    except ValueError:
        rel_path = file_path # Fallback if not relative to cwd
    
    # Extract models defined in the yaml
    if 'models' in content and isinstance(content['models'], list):
        for model in content['models']:
            name = model.get('name')
            if name:
                description = model.get('description', '')
                
                # Extract columns and their descriptions
                columns = []
                col_descriptions = {}
                if 'columns' in model and isinstance(model['columns'], list):
                    for col in model['columns']:
                        col_name = col.get('name')
                        if col_name:
                            columns.append(col_name)
                            col_desc = col.get('description', '')
                            if col_desc:
                                col_descriptions[col_name] = col_desc
                
                nodes.append(DatasetNode(
                    id=name,
                    source_file=rel_path,
                    source_line=None,
                    dataset_type="model",
                    columns=columns,
                    description=description,
                    column_descriptions=col_descriptions
                ))
    
    # Extract sources if present
    if 'sources' in content and isinstance(content['sources'], list):
        for source in content['sources']:
            source_name = source.get('name')
            source_desc = source.get('description', '')
            if 'tables' in source and isinstance(source['tables'], list):
                for table in source['tables']:
                    table_name = table.get('name')
                    table_desc = table.get('description', source_desc)
                    if table_name:
                        columns = []
                        col_descriptions = {}
                        if 'columns' in table and isinstance(table['columns'], list):
                            for col in table['columns']:
                                col_name = col.get('name')
                                if col_name:
                                    columns.append(col_name)
                                    c_desc = col.get('description', '')
                                    if c_desc:
                                        col_descriptions[col_name] = c_desc
                        
                        nodes.append(DatasetNode(
                            id=table_name,
                            source_file=rel_path,
                            source_line=None,
                            dataset_type="source",
                            columns=columns,
                            description=table_desc,
                            column_descriptions=col_descriptions
                        ))
                    
    return nodes

def extract_project_metadata(project_yml_path: str) -> Dict[str, Any]:
    """
    Parses dbt_project.yml to extract project-level configuration.
    Specifically pulls the project name and path settings (models, seeds).
    """
    content = parse_yaml_file(project_yml_path)
    project_name = content.get('name')
    
    # dbt defaults if not specified
    model_paths = content.get('model-paths') or content.get('source-paths') or ['models']
    seed_paths = content.get('seed-paths') or ['seeds']
    
    result = {
        "node": None,
        "model_paths": model_paths,
        "seed_paths": seed_paths
    }
    
    if project_name:
        try:
            rel_path = str(Path(project_yml_path).relative_to(Path.cwd()))
        except ValueError:
            rel_path = project_yml_path
            
        result["node"] = ModuleNode(
            id=project_name,
            source_file=rel_path,
            source_line=None,
            file_type="yaml",
            logical_name=project_name
        )
    return result

def analyze_all_yaml_files(root_dir: str) -> Dict[str, Any]:
    """
    Finds and parses all relevant dbt YAML files in the project.
    Now dynamically resolves model paths from dbt_project.yml.
    """
    results = {
        "project": None,
        "model_paths": ["models"],
        "seed_paths": ["seeds"],
        "datasets": []
    }
    
    # 1. Parse dbt_project.yml to get configurations
    project_path = os.path.join(root_dir, "dbt_project.yml")
    if os.path.exists(project_path):
        proj_metadata = extract_project_metadata(project_path)
        results["project"] = proj_metadata["node"]
        results["model_paths"] = proj_metadata["model_paths"]
        results["seed_paths"] = proj_metadata["seed_paths"]
        print(f"[YAML Parser] Processed config: {project_path}")
    
    # 2. Find and parse all schema.yml files within model and seed paths
    search_dirs = results["model_paths"] + results["seed_paths"]
    
    for sub_dir in search_dirs:
        target_dir = os.path.join(root_dir, sub_dir)
        if not os.path.exists(target_dir):
            continue
            
        try:
            for root, dirs, files in os.walk(target_dir):
                for file in files:
                    # dbt allows files ending in .yml or .yaml
                    if file.endswith(".yml") or file.endswith(".yaml"):
                        full_path = os.path.join(root, file)
                        # We only care about schema files (which often contain 'models' or 'sources')
                        nodes = extract_nodes_from_schema_yml(full_path)
                        if nodes:
                            results["datasets"].extend(nodes)
                            print(f"[YAML Parser] Processed: {full_path} (found {len(nodes)} models/sources)")
        except Exception as e:
            print(f"[WARNING] Error walking directory {target_dir}: {e}")
                
    return results

if __name__ == "__main__":
    # Test on the jaffle_shop directory
    target_repo = "jaffle_shop"
    if os.path.exists(target_repo):
        print(f"Testing YAML Parser on {target_repo}...")
        parsed_data = analyze_all_yaml_files(target_repo)
        
        if parsed_data["project"]:
            print(f"\nProject Name: {parsed_data['project'].logical_name}")
            
        print(f"\nTotal Datasets parsed: {len(parsed_data['datasets'])}")
        for ds in parsed_data["datasets"]:
            print(f"- {ds.id} ({ds.dataset_type}) with {len(ds.columns)} columns")
    else:
        print(f"[ERROR] Could not find {target_repo} directory.")
