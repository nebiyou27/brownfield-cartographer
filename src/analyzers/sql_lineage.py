import os
import re
import glob
from typing import List, Set
from pathlib import Path

import sqlglot
from sqlglot import exp

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.models.schemas import TransformationEdge

def strip_jinja(sql: str) -> str:
    """
    Clean dbt Jinja templating from SQL before parsing.
    
    dbt uses three types of Jinja tags:
    - {{ ref('table') }}      → replace with just: table
    - {{ source('s','t') }}   → replace with just: t  
    - {% set x = ... %}       → remove entirely (logic blocks)
    - {# this is a comment #} → remove entirely (comments)
    """
    
    # Step 1: Replace {{ ref('table_name') }} with just table_name
    # re.sub finds the pattern and replaces it with the captured group \1
    sql = re.sub(r"\{\{\s*ref\(['\"](\w+)['\"]\)\s*\}\}", r"\1", sql)
    
    # Step 2: Replace {{ source('schema', 'table_name') }} with just table_name
    # Takes the last argument inside source()
    sql = re.sub(r"\{\{\s*source\(['\"\w]+,\s*['\"](\w+)['\"]\)\s*\}\}", r"\1", sql)
    
    # Step 3: Remove {# comment blocks #} entirely
    sql = re.sub(r'\{#.*?#\}', '', sql, flags=re.DOTALL)
    
    # Step 4: Remove {% logic blocks %} entirely (set, if, for, macro etc.)
    sql = re.sub(r'\{%.*?%\}', '', sql, flags=re.DOTALL)
    
    # Step 5: Remove any remaining {{ }} that we did not handle above
    sql = re.sub(r'\{\{.*?\}\}', '', sql, flags=re.DOTALL)
    
    return sql.strip()

def get_lineage_from_sql(sql: str, target_name: str, source_file: str) -> List[TransformationEdge]:
    """
    Parse cleaned SQL and return TransformationEdge list.
    
    Strategy:
    1. Parse the full SQL with sqlglot
    2. Collect all CTE names defined in WITH clause (these are NOT real tables)
    3. Collect all table references from FROM and JOIN clauses
    4. Keep only tables that are NOT CTEs = these are real upstream tables
    5. Return one TransformationEdge per real upstream table
    """
    edges = []
    
    try:
        # Parse the SQL - use read='duckdb' dialect which handles dbt SQL well
        statements = sqlglot.parse(sql, read='duckdb')
        
        for statement in statements:
            if statement is None:
                continue
                
            # Step 1: collect all CTE alias names (temporary query aliases)
            # CTEs are defined in the WITH clause like: WITH my_cte AS (SELECT ...)
            # These are NOT real tables, so we must ignore them later.
            cte_names = set()
            for cte in statement.find_all(exp.CTE):
                if cte.alias:
                    cte_names.add(cte.alias.lower())
            
            # Step 2: collect ALL table references in the statement
            all_tables = set()
            for table in statement.find_all(exp.Table):
                if table.name:
                    all_tables.add(table.name.lower())
            
            # Step 3: real sources = tables that are NOT defined as CTEs in this query
            real_sources = all_tables - cte_names
            
            # Step 4: also remove the target itself if it appears (self-references or simple aliases)
            real_sources.discard(target_name.lower())
            
            # Step 5: create one edge per real source table
            for source in real_sources:
                edges.append(TransformationEdge(
                    source_dataset=source,
                    target_dataset=target_name,
                    source_file=source_file,
                    transformation_type="sql_select"
                ))
    
    except Exception as e:
        print(f"[WARNING] Could not extract lineage from {source_file}: {e}")
    
    return edges

def analyze_sql_file(file_path: str) -> List[TransformationEdge]:
    """Read a .sql file, strip Jinja, parse SQL, return lineage edges."""
    
    print(f"[SQL Lineage] Processing: {file_path}")
    
    try:
        # Read the raw file content
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_sql = f.read()
        
        # Strip all Jinja templating before parsing
        clean_sql = strip_jinja(raw_sql)
        
        # Target table name = the filename without .sql extension
        target_name = Path(file_path).stem
        
        # Get relative path for cleaner output (ensuring we handle paths correctly on Windows)
        cwd = Path.cwd()
        rel_path = str(Path(file_path).relative_to(cwd))
        
        # Extract lineage edges using the logic above
        return get_lineage_from_sql(clean_sql, target_name, rel_path)
        
    except Exception as e:
        print(f"[WARNING] Could not read {file_path}: {e}")
        return []

def analyze_all_sql_files(models_dir: str) -> List[TransformationEdge]:
    """
    Walks a directory to find all .sql files, runs the analyzer on each,
    and aggregates all extracted data lineage edges into a single list.
    """
    all_edges = []
    
    search_pattern = os.path.join(models_dir, "**", "*.sql")
    sql_files = glob.glob(search_pattern, recursive=True)
    
    for file_path in sql_files:
        edges = analyze_sql_file(file_path)
        all_edges.extend(edges)
        
    return all_edges

# Simple test script for verified execution
if __name__ == "__main__":
    test_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "jaffle_shop", "models"))
    
    if os.path.exists(test_dir):
        print(f"Testing analyzer on {test_dir}...")
        extracted_edges = analyze_all_sql_files(test_dir)
        
        print("\n=== Extracted Lineage ===")
        for edge in extracted_edges:
            print(f"{edge.source_dataset} -> {edge.target_dataset} (from {edge.source_file})")
    else:
        print(f"Could not find test directory {test_dir}. Run from project root.")
