import os
import re
import glob
from pathlib import Path
from typing import List, Set

import sqlglot
from sqlglot import exp

import sys
# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.models.schemas import TransformationEdge

def strip_jinja(sql: str) -> str:
    """
    Clean dbt Jinja templating from SQL before parsing.
    """
    # Replace {{ ref('table_name') }} with just table_name
    sql = re.sub(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}", r"\1", sql)
    
    # Replace {{ source('schema', 'table_name') }} with last arg
    sql = re.sub(r"\{\{\s*source\(['\"\w]+,\s*['\"](\w+)['\"]\)\s*\}\}", r"\1", sql)
    
    # Remove {# comment blocks #} entirely
    sql = re.sub(r'\{#.*?#\}', '', sql, flags=re.DOTALL)
    
    # Remove {% logic blocks %} entirely
    sql = re.sub(r'\{%.*?%\}', '', sql, flags=re.DOTALL)
    
    # Remove any remaining {{ }}
    sql = re.sub(r'\{\{.*?\}\}', '', sql, flags=re.DOTALL)
    
    return sql.strip()

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

def analyze_sql_file(file_path: str) -> List[TransformationEdge]:
    """
    Reads a single .sql file and extracts lineage edges.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw_sql = f.read()
    except Exception as e:
        print(f"[WARNING] Could not read file {file_path}: {e}")
        return []

    clean_sql = strip_jinja(raw_sql)
    target_name = Path(file_path).stem
    
    try:
        rel_path = str(Path(file_path).relative_to(Path.cwd()))
    except ValueError:
        rel_path = file_path

    return get_lineage_from_sql(clean_sql, target_name, rel_path)

def analyze_all_sql_files(models_dir: str) -> List[TransformationEdge]:
    """
    Recursively finds all .sql files in a directory and extracts lineage.
    """
    all_edges = []
    if not os.path.exists(models_dir):
        return []
        
    try:
        # Recursively find all .sql files
        sql_files = glob.glob(os.path.join(models_dir, "**", "*.sql"), recursive=True)
        
        for file_path in sql_files:
            print(f"[SQL Lineage] Processing: {file_path}")
            edges = analyze_sql_file(file_path)
            all_edges.extend(edges)
    except Exception as e:
        print(f"[WARNING] Error analyzing directory {models_dir}: {e}")
            
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
