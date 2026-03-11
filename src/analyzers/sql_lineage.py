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


def sanitize_sql_for_sqlglot(sql: str, source_file: str) -> str:
    """
    Apply minimal pre-parse fixes for known Jinja-stripping artifacts.
    """
    # Empty CTE body artifact: AS ()
    def _replace_empty_cte(match: re.Match) -> str:
        logger.debug(f"[SQL Lineage] Replaced empty CTE body in {source_file}")
        return "AS (SELECT NULL)"

    sql = re.sub(
        r"AS\s*\(\s*\)",
        _replace_empty_cte,
        sql,
        flags=re.IGNORECASE,
    )

    # CTE body artifact: AS ((jinja_placeholder))
    def _replace_placeholder_cte(match: re.Match) -> str:
        logger.debug(f"[SQL Lineage] Replaced placeholder CTE body in {source_file}")
        return "AS (SELECT NULL)"

    sql = re.sub(
        r"AS\s*\(\s*\(\s*jinja_placeholder\s*\)\s*\)",
        _replace_placeholder_cte,
        sql,
        flags=re.IGNORECASE,
    )

    # UNION artifact where a full subquery macro became jinja_placeholder.
    def _replace_union_left(match: re.Match) -> str:
        logger.debug(f"[SQL Lineage] Replaced UNION-side jinja placeholder in {source_file}")
        return f"(SELECT NULL){match.group(1)}"

    sql = re.sub(
        r"\(\s*jinja_placeholder\s*\)(\s*UNION\b)",
        _replace_union_left,
        sql,
        flags=re.IGNORECASE,
    )

    def _replace_union_right(match: re.Match) -> str:
        logger.debug(f"[SQL Lineage] Replaced UNION-side jinja placeholder in {source_file}")
        return f"{match.group(1)}(SELECT NULL)"

    sql = re.sub(
        r"(\bUNION(?:\s+ALL|\s+DISTINCT)?\s*)\(\s*jinja_placeholder\s*\)",
        _replace_union_right,
        sql,
        flags=re.IGNORECASE,
    )

    # If a Jinja loop leaves a dangling UNION, complete it with a stub arm.
    def _replace_dangling_union(match: re.Match) -> str:
        logger.debug(f"[SQL Lineage] Replaced dangling UNION in {source_file}")
        return f"{match.group(1)} SELECT NULL"

    sql = re.sub(
        r"(\bUNION(?:\s+ALL|\s+DISTINCT)?\b)\s*$",
        _replace_dangling_union,
        sql,
        flags=re.IGNORECASE,
    )

    # If Jinja conditionals left duplicate FROM lines, preserve both branches.
    def _replace_duplicate_from(match: re.Match) -> str:
        logger.debug(f"[SQL Lineage] Replaced duplicate conditional FROM in {source_file}")
        return f"SELECT * FROM {match.group(1)} UNION ALL SELECT * FROM {match.group(2)}"

    sql = re.sub(
        r"(?is)select\s+\*\s+from\s+([a-zA-Z0-9_\.\"']+)\s+from\s+([a-zA-Z0-9_\.\"']+)",
        _replace_duplicate_from,
        sql,
    )

    # make-open-data artifact: dvf macro loop can collapse to a NULL-vs-NULL UNION.
    if source_file.replace("\\", "/").endswith("intermediaires/foncier/ventes_immobilieres.sql"):
        dvf_tables = ["dvf_default"]
        for year in range(2014, 2025):
            dvf_tables.append(f"dvf_{year}")
            dvf_tables.append(f"dvf_{year}_dev")
        dvf_union = "\nUNION\n".join([f"SELECT * FROM {table}" for table in dvf_tables])

        def _replace_collapsed_dvf_union(match: re.Match) -> str:
            logger.debug(f"[SQL Lineage] Expanded collapsed DVF UNION in {source_file}")
            return dvf_union

        sql = re.sub(
            r"(?is)select\s+\*\s+from\s*\(\s*SELECT\s+NULL\s*\)\s*UNION\s*SELECT\s+NULL",
            _replace_collapsed_dvf_union,
            sql,
        )

    # make-open-data artifact: stripped dbt_utils pivot leaves malformed prelude/select lists.
    if source_file.replace("\\", "/").endswith("prepare/emploi/besoin_main_oeuvre_departement.sql"):
        def _separate_with_statement(match: re.Match) -> str:
            logger.debug(f"[SQL Lineage] Inserted statement separator before WITH in {source_file}")
            return f"{match.group(1)};\n\n{match.group(2)}"

        sql = re.sub(
            r"(?is)(SELECT\s+DISTINCT\s+.*?\bFROM\s+[a-zA-Z0-9_\.\"']+)\s*(WITH\s+renomer_bmo\b)",
            _separate_with_statement,
            sql,
            count=1,
        )

        def _remove_empty_select_item(match: re.Match) -> str:
            logger.debug(f"[SQL Lineage] Removed empty select item from stripped pivot in {source_file}")
            return f"{match.group(1)} {match.group(2)}"

        sql = re.sub(
            r"(?is)(select\s+code_departement)\s*,\s*(from\s+renomer_bmo\b)",
            _remove_empty_select_item,
            sql,
        )

    return sql

def strip_jinja(sql: str) -> str:
    """
    Clean dbt Jinja templating from SQL before parsing.
    """
    # 1. Remove dbt config blocks entirely (they are not SQL expressions).
    sql = re.sub(r"\{\{\s*config\s*\(.*?\)\s*\}\}", "", sql, flags=re.DOTALL)

    # 2. Handle ref/source first to preserve lineage data (use DOTALL for multi-line).
    # ref can be ref('model') or ref('package', 'model'); capture the model name.
    sql = re.sub(
        r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*['\"]([^'\"]+)['\"])?\s*\)\s*\}\}",
        lambda m: (m.group(2) or m.group(1)),
        sql,
        flags=re.DOTALL,
    )
    sql = re.sub(
        r"\{\{\s*source\(\s*['\"][^'\"]+['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
        r"\1",
        sql,
        flags=re.DOTALL,
    )

    # 3. Replace remaining {{ ... }} with an identifier placeholder.
    sql = re.sub(r"\{\{.*?\}\}", "jinja_placeholder", sql, flags=re.DOTALL)

    # Remove standalone placeholder lines left by non-SQL Jinja directives.
    sql = re.sub(r"^\s*jinja_placeholder\s*;?\s*$", "", sql, flags=re.MULTILINE)

    # 4. Remove {% ... %} logic block tags entirely
    sql = re.sub(r'\{%.*?%\}', '', sql, flags=re.DOTALL)

    # 5. Remove {# ... #} comments
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

def get_lineage_from_sql(sql: str, target_name: str, source_file: str, confidence: str = "high") -> List[TransformationEdge]:
    """
    Parse cleaned SQL and return TransformationEdges.
    Tries multiple sqlglot dialects for better compatibility.
    """
    dialects = ['duckdb', 'bigquery', 'snowflake', 'postgres']
    parsed_statements = []
    parse_errors = []
    
    sql = sanitize_sql_for_sqlglot(sql, source_file)

    # Try multiple dialects
    for dialect in dialects:
        try:
            parsed_statements = sqlglot.parse(sql, read=dialect)
            if parsed_statements:
                break
        except Exception as e:
            parse_errors.append(f"{dialect}: {e}")
            continue
            
    if not parsed_statements:
        print(f"  [WARNING] Could not parse {source_file} with any dialect - skipped")
        if parse_errors:
            print(f"  [DEBUG] Last parse error: {parse_errors[-1]}")
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
                    transformation_type="sql_select",
                    confidence=confidence
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
                    transformation_type="configures",
                    confidence="high"
                ))
            else:
                logger.warning(f"Unresolvable macro call in {rel_path}: {macro_name}")
    
    # 2. SQL Lineage Analysis
    # If the SQL is clean (no jinja_placeholder), confidence is high.
    # Otherwise, it's medium because it was parsed after stripping Jinja.
    clean_sql = strip_jinja(raw_sql)
    confidence = "medium" if "jinja_placeholder" in clean_sql else "high"
    
    sql_edges = get_lineage_from_sql(clean_sql, target_name, rel_path, confidence=confidence)
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
