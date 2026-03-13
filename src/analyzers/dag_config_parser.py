import os
import re
import sys
from pathlib import Path
from typing import Any

import yaml

# Add project root to sys.path for absolute imports when running as script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.logger import get_logger
from src.models.schemas import DatasetNode, ModuleNode

logger = get_logger(__name__)


class ParseFailureDatasetNode:
    """
    Minimal node emitted when a YAML file fails to load/parse.
    """

    def __init__(self, file_id: str, reason: str):
        self.id = file_id
        self.source_file = file_id
        self.source_line = None
        self.description = ""
        self.dataset_type = "yaml_file"
        self.columns: list[str] = []
        self.column_descriptions: dict[str, str] = {}
        self.parsed = False
        self.reason = reason

    def model_dump(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_file": self.source_file,
            "source_line": self.source_line,
            "description": self.description,
            "dataset_type": self.dataset_type,
            "columns": self.columns,
            "column_descriptions": self.column_descriptions,
            "parsed": self.parsed,
            "reason": self.reason,
        }


def _relative_file_path(file_path: str) -> str:
    try:
        return str(Path(file_path).relative_to(Path.cwd()))
    except ValueError:
        return file_path


def parse_yaml_file(file_path: str) -> tuple[dict[str, Any], str | None]:
    """
    Safely load a YAML file and return its content as a dictionary.
    Handles potential parsing errors gracefully.
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()
            # Fix YAML parser to handle tab characters gracefully by replacing them with 4 spaces
            content = content.replace("\t", "    ")
            data = yaml.safe_load(content)
            return (data if isinstance(data, dict) else {}), None
    except Exception as e:
        logger.warning("Could not parse YAML file %s: %s", file_path, e)
        reason = f"yaml parse failure ({e.__class__.__name__}: {str(e).splitlines()[0][:120]})"
        return {}, reason


def load_doc_blocks(models_dir: str) -> dict[str, str]:
    """
    Read the docs.md file and extract all doc blocks into a dictionary.
    """
    doc_blocks = {}
    docs_path = Path(models_dir) / "docs.md"

    if not docs_path.exists():
        return doc_blocks

    try:
        content = docs_path.read_text(encoding="utf-8")
        pattern = r"\{%\s*docs\s+(\w+)\s*%\}(.*?)\{%\s*enddocs\s*%\}"
        matches = re.findall(pattern, content, flags=re.DOTALL)

        for block_name, block_content in matches:
            doc_blocks[block_name.strip()] = block_content.strip()
            logger.info("Loaded doc block: %s", block_name.strip())
    except Exception as e:
        logger.warning("Could not read docs.md: %s", e)

    return doc_blocks


def resolve_doc_references(description: str, doc_blocks: dict[str, str]) -> str:
    """
    Replace {{ doc("block_name") }} with the actual text from doc_blocks.
    """
    if not description or not doc_blocks:
        return description

    pattern = r'\{\{\s*doc\(["\'](\w+)["\']\)\s*\}\}'
    matches = re.findall(pattern, description)

    for block_name in matches:
        if block_name in doc_blocks:
            # Escape potential regex special characters in the replacement if any,
            # though here we just want the literal string.
            replacement = doc_blocks[block_name]
            # Use re.escape briefly for the search pattern but not the replacement
            search_pattern = rf'\{{\{{\s*doc\(["\']{block_name}["\']\)\s*\}}\}}'
            description = re.sub(search_pattern, replacement, description)
        else:
            logger.warning("doc() block '%s' not found in docs.md", block_name)

    return description


def extract_nodes_from_schema_yml(
    file_path: str, doc_blocks: dict[str, str] = None
) -> list[DatasetNode]:
    """
    Parses a dbt schema.yml file to extract DatasetNodes.
    Extracts model names, descriptions, and column details.
    """
    nodes = []
    content, parse_error = parse_yaml_file(file_path)

    # Get relative path for traceability
    rel_path = _relative_file_path(file_path)

    if parse_error:
        nodes.append(ParseFailureDatasetNode(rel_path, parse_error))
        return nodes

    # Extract models defined in the yaml
    if "models" in content and isinstance(content["models"], list):
        for model in content["models"]:
            name = model.get("name")
            if name:
                raw_description = model.get("description", "")
                description = resolve_doc_references(raw_description, doc_blocks)

                # Extract columns and their descriptions
                columns = []
                col_descriptions = {}
                if "columns" in model and isinstance(model["columns"], list):
                    for col in model["columns"]:
                        col_name = col.get("name")
                        if col_name:
                            columns.append(col_name)
                            raw_col_desc = col.get("description", "")
                            col_desc = resolve_doc_references(raw_col_desc, doc_blocks)
                            if col_desc:
                                col_descriptions[col_name] = col_desc

                nodes.append(
                    DatasetNode(
                        id=name,
                        source_file=rel_path,
                        source_line=None,
                        dataset_type="model",
                        columns=columns,
                        description=description,
                        column_descriptions=col_descriptions,
                    )
                )

    # Extract sources if present
    if "sources" in content and isinstance(content["sources"], list):
        for source in content["sources"]:
            source.get("name")
            raw_source_desc = source.get("description", "")
            source_desc = resolve_doc_references(raw_source_desc, doc_blocks)

            if "tables" in source and isinstance(source["tables"], list):
                for table in source["tables"]:
                    table_name = table.get("name")
                    raw_table_desc = table.get("description", source_desc)
                    table_desc = resolve_doc_references(raw_table_desc, doc_blocks)
                    if table_name:
                        columns = []
                        col_descriptions = {}
                        if "columns" in table and isinstance(table["columns"], list):
                            for col in table["columns"]:
                                col_name = col.get("name")
                                if col_name:
                                    columns.append(col_name)
                                    raw_c_desc = col.get("description", "")
                                    c_desc = resolve_doc_references(raw_c_desc, doc_blocks)
                                    if c_desc:
                                        col_descriptions[col_name] = c_desc

                        nodes.append(
                            DatasetNode(
                                id=table_name,
                                source_file=rel_path,
                                source_line=None,
                                dataset_type="source",
                                columns=columns,
                                description=table_desc,
                                column_descriptions=col_descriptions,
                            )
                        )

    return nodes


def extract_project_metadata(project_yml_path: str) -> dict[str, Any]:
    """
    Parses dbt_project.yml to extract project-level configuration.
    Specifically pulls the project name and path settings (models, seeds).
    """
    content, _ = parse_yaml_file(project_yml_path)
    project_name = content.get("name")

    # dbt defaults if not specified
    model_paths = content.get("model-paths") or content.get("source-paths") or ["models"]
    seed_paths = content.get("seed-paths") or ["seeds"]
    macro_paths = content.get("macro-paths") or ["macros"]

    result = {
        "node": None,
        "model_paths": model_paths,
        "seed_paths": seed_paths,
        "macro_paths": macro_paths,
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
            logical_name=project_name,
        )
    return result


def analyze_all_yaml_files(root_dir: str) -> dict[str, Any]:
    """
    Finds and parses all relevant dbt YAML files in the project.
    Now dynamically resolves model paths from dbt_project.yml.
    """
    results = {
        "project": None,
        "model_paths": ["models"],
        "seed_paths": ["seeds"],
        "macro_paths": ["macros"],
        "datasets": [],
    }

    # 1. Parse dbt_project.yml to get configurations
    project_path = os.path.join(root_dir, "dbt_project.yml")
    if os.path.exists(project_path):
        proj_metadata = extract_project_metadata(project_path)
        results["project"] = proj_metadata["node"]
        results["model_paths"] = proj_metadata["model_paths"]
        results["seed_paths"] = proj_metadata["seed_paths"]
        results["macro_paths"] = proj_metadata["macro_paths"]
        logger.info("Processed config: %s", project_path)
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
            print(f"\n- {ds.id} ({ds.dataset_type})")
            if ds.id == "orders":
                print(
                    f"  [DEBUG] Orders status description: {ds.column_descriptions.get('status')}"
                )
    else:
        print(f"[ERROR] Could not find {target_repo} directory.")
