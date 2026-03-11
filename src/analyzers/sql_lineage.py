import os
import re
import glob
from pathlib import Path
import logging
from typing import List, Set, Optional, Dict

import sqlglot
from sqlglot import exp

import sys
# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.models.schemas import TransformationEdge

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def strip_jinja(sql: str) -> str:
    """
    Clean dbt Jinja templating from SQL before parsing.
    """
    # 1. Handle ref/source first to preserve lineage data (use DOTALL for multi-line)
    sql = re.sub(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}", r"\1", sql, flags=re.DOTALL)
    sql = re.sub(r"\{\{\s*source\(['\"\w]+,\s*['\"](\w+)['\"]\)\s*\}\}", r"\1", sql, flags=re.DOTALL)

    # 2. Replace remaining {{ ... }} with the token 'jinja_placeholder'
    sql = re.sub(r"\{\{.*?\}\}", "jinja_placeholder", sql, flags=re.DOTALL)
    
    # 3. Remove {% ... %} logic block tags entirely
    sql = re.sub(r'\{%.*?%\}', '', sql, flags=re.DOTALL)

    # 4. Remove {# ... #} comments
    sql = re.sub(r'\{#.*?#\}', '', sql, flags=re.DOTALL)
    
    return sql.strip()

def extract_macro_calls(sql: str) -> List[str]:
    """
    Extract all {{ macro_name(...) }} calls from raw SQL.
    """
    # Matches {{ macro_name(...) }} and captures macro_name
    # Excludes common built-ins like ref, source, config
    pattern = r"\{\{\s*(\w+)\s*\("
    matches = re.findall(pattern, sql)
    
    built_ins = {'ref', 'source', 'config', 'var', 'env_var'}
    return [m for m in matches if m not in built_ins]

def get_macros_map(macros_dir: str) -> Dict[str, str]:
    """
    Builds a map of macro names to their file paths in 5_macros/.
    """
    macro_map = {}
    if not os.path.exists(macros_dir):
        return macro_map
        
    sql_files = glob.glob(os.path.join(macros_dir, "**", "*.sql"), recursive=True)
    for file_path in sql_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
                # Find {% macro macro_name(...) %}
                matches = re.findall(r"\{%\s*macro\s+(\w+)\s*\(", content)
                for macro_name in matches:
                    try:
                        rel_path = str(Path(file_path).relative_to(Path.cwd()))
                    except ValueError:
                        rel_path = file_path
                    macro_map[macro_name] = rel_path
        except Exception as e:
            logger.warning(f"Could not read macro file {file_path}: {e}")
            
    return macro_map

def get_lineage_from_sql(sql: str, target_name: str, source_file: str) -> List[TransformationEdge]:
    """
    Parse cleaned SQL and return TransformationEdges.
    Tries multiple sqlglot dialects for better compatibility.
    """
    dialects = ['duckdb', 'bigquery', 'snowflake', 'postgres']
    parsed_statements = []
    
    # Try multiple dialects
    for dialect in dialects:
        try:
            parsed_statements = sqlglot.parse(sql, read=dialect)
            if parsed_statements:
                break
        except Exception:
            continue
            
    if not parsed_statements:
        print(f"  [WARNING] Could not parse {source_file} with any dialect - skipped")
        return []

    edges = []
    for statement in parsed_statements:
        if not statement:
            continue
            
        # 1. Extract all CTE names so we can filter them out
        cte_names = set()
        for cte in statement.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias.lower())

        # 2. Extract all Table references
        for table in statement.find_all(exp.Table):
            if not table.name:
                continue
            table_name = table.name.lower()
            
            # Filter: No CTEs, no self-reference
            if table_name not in cte_names and table_name != target_name.lower():
                edges.append(TransformationEdge(
                    source_dataset=table_name,
                    target_dataset=target_name,
                    source_file=source_file,
                    source_line=None,
                    transformation_type="sql_select"
                ))
            
    return edges

def analyze_sql_file(file_path: str, macro_map: Optional[Dict[str, str]] = None) -> List[TransformationEdge]:
    """
    Reads a single .sql file and extracts lineage edges, including macro dependencies.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_sql = f.read()
    except Exception as e:
        logger.warning(f"Could not read file {file_path}: {e}")
        return []

    target_name = Path(file_path).stem
    try:
        rel_path = str(Path(file_path).relative_to(Path.cwd()))
    except ValueError:
        rel_path = file_path

    edges = []
    
    # 1. Macro Analysis
    macro_calls = extract_macro_calls(raw_sql)
    if macro_map:
        for macro_name in macro_calls:
            macro_file = macro_map.get(macro_name)
            if macro_file:
                edges.append(TransformationEdge(
                    source_dataset=rel_path,
                    target_dataset=macro_file,
                    source_file=rel_path,
                    source_line=None,
                    transformation_type="configures"
                ))
            else:
                logger.warning(f"Unresolvable macro call in {rel_path}: {macro_name}")
    
    # 2. SQL Lineage Analysis
    clean_sql = strip_jinja(raw_sql)
    sql_edges = get_lineage_from_sql(clean_sql, target_name, rel_path)
    edges.extend(sql_edges)

    return edges

def analyze_all_sql_files(models_dir: str, macros_dir: Optional[str] = None) -> List[TransformationEdge]:
    """
    Recursively finds all .sql files in a directory and extracts lineage.
    """
    all_edges = []
    if not os.path.exists(models_dir):
        return []

    macro_map = {}
    if macros_dir:
        logger.info(f"Building macro map from: {macros_dir}")
        macro_map = get_macros_map(macros_dir)
        
    try:
        # Recursively find all .sql files
        sql_files = glob.glob(os.path.join(models_dir, "**", "*.sql"), recursive=True)
        
        for file_path in sql_files:
            logger.info(f"[SQL Lineage] Processing: {file_path}")
            edges = analyze_sql_file(file_path, macro_map)
            all_edges.extend(edges)
    except Exception as e:
        logger.warning(f"Error analyzing directory {models_dir}: {e}")
            
    return all_edges

if __name__ == "__main__":
    # Test script
    test_repo = "jaffle_shop"
    test_dir = os.path.join(test_repo, "models")
    
    if os.path.exists(test_dir):
        print(f"Testing analyzer on {test_dir}...")
        extracted_edges = analyze_all_sql_files(test_dir)
        
        print("\n=== Extracted Lineage ===")
        for edge in extracted_edges:
            print(f"{edge.source_dataset} -> {edge.target_dataset} (from {edge.source_file})")
    else:
        print(f"Could not find test directory {test_dir}.")
