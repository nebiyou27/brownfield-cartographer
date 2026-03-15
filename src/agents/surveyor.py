import os
import re
from pathlib import Path

from ..analyzers.dag_config_parser import analyze_all_yaml_files
from ..analyzers.git_analyzer import get_all_file_velocities, get_git_change_velocity
from ..analyzers.tree_sitter_analyzer import LanguageRouter
from ..graph.knowledge_graph import KnowledgeGraph
from ..logger import get_logger
from ..models.schemas import MacroNode, ModuleNode
from ..path_utils import normalize_path_key, with_path_aliases

logger = get_logger(__name__)


class Surveyor:
    """
    Scans the repository, builds a module/file dependency graph,
    identifies which files import which, and computes git change velocity.
    """

    def __init__(self, repo_path: str):
        self.repo_path = repo_path

    @staticmethod
    def _parse_macro_args(raw_args: str) -> list[str]:
        args: list[str] = []
        for token in raw_args.split(","):
            value = token.strip()
            if not value:
                continue
            value = value.split("=")[0].strip()
            if value.startswith("*"):
                value = value.lstrip("*")
            if value:
                args.append(value)
        return args

    def _register_macro_nodes(
        self,
        graph: KnowledgeGraph,
        results: dict,
        velocity_resolver,
    ) -> None:
        macro_paths = results.get("macro_paths", []) or []
        macro_pattern = re.compile(
            r"\{%\s*macro\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\)\s*%\}",
            flags=re.IGNORECASE | re.DOTALL,
        )
        for rel_macro_dir in macro_paths:
            macro_dir = Path(self.repo_path) / rel_macro_dir
            if not macro_dir.exists():
                continue
            for sql_file in macro_dir.rglob("*.sql"):
                try:
                    content = sql_file.read_text(encoding="utf-8", errors="replace")
                except Exception:
                    continue

                normalized_file = normalize_path_key(str(sql_file.relative_to(self.repo_path)))
                velocity = velocity_resolver(normalized_file)

                for match in macro_pattern.finditer(content):
                    macro_name = match.group(1).strip()
                    macro_args = self._parse_macro_args(match.group(2))
                    source_line = content.count("\n", 0, match.start()) + 1
                    macro_id = f"macro:{macro_name}:{normalized_file}:{source_line}"
                    macro_node = MacroNode(
                        id=macro_id,
                        source_file=normalized_file,
                        source_line=source_line,
                        logical_name=macro_name,
                        macro_args=macro_args,
                    )
                    graph.add_node(macro_node)
                    graph.graph.nodes[macro_id]["git_change_velocity"] = velocity

    def _register_ingestion_config_nodes(self, graph: KnowledgeGraph, velocity_resolver) -> None:
        config_roles = {
            "source_to_storage.yml": "extraction_config",
            "storage_to_pg.yml": "loading_config",
        }
        repo_root = Path(self.repo_path)
        for file_name, role in config_roles.items():
            for config_path in repo_root.rglob(file_name):
                rel_path = normalize_path_key(str(config_path.relative_to(repo_root)))
                config_node = ModuleNode(
                    id=rel_path,
                    source_file=rel_path,
                    source_line=None,
                    file_type="yaml",
                    logical_name=config_path.stem,
                )
                graph.add_node(config_node)
                graph.graph.nodes[rel_path]["ingestion_role"] = role
                graph.graph.nodes[rel_path]["git_change_velocity"] = velocity_resolver(rel_path)

    def survey(self, graph: KnowledgeGraph):
        logger.info("Scanning project structure and YAML configs...")
        results = analyze_all_yaml_files(self.repo_path)
        results["git_velocity"] = get_all_file_velocities(self.repo_path)

        def _velocity_for_path(file_path: str) -> int:
            normalized = normalize_path_key(file_path)
            if normalized in results["git_velocity"]:
                return int(results["git_velocity"][normalized])
            velocity = get_git_change_velocity(self.repo_path, normalized)
            with_path_aliases(results["git_velocity"], normalized, velocity)
            return velocity

        # Add the project node
        if results["project"]:
            graph.add_node(results["project"])

        # Add all datasets (models/sources) as modules
        source_schema_files: set[str] = set()
        for node in results["datasets"]:
            repo_abs = os.path.abspath(self.repo_path)
            file_abs = os.path.abspath(os.path.join(self.repo_path, node.source_file))
            file_in_repo = normalize_path_key(os.path.relpath(file_abs, repo_abs))

            velocity = _velocity_for_path(file_in_repo)

            graph.add_node(node)
            graph.graph.nodes[node.id]["git_change_velocity"] = velocity
            with_path_aliases(results["git_velocity"], file_in_repo, velocity)

            if getattr(node, "dataset_type", "") == "source" and os.path.basename(
                file_in_repo
            ).lower() in ("schema.yml", "schema.yaml"):
                source_schema_files.add(file_in_repo)

        for schema_file in sorted(source_schema_files):
            schema_node = ModuleNode(
                id=schema_file,
                source_file=schema_file,
                source_line=None,
                file_type="yaml",
                logical_name="dbt_sources_schema",
            )
            graph.add_node(schema_node)
            graph.graph.nodes[schema_file]["ingestion_role"] = "dbt_sources_schema"
            graph.graph.nodes[schema_file]["git_change_velocity"] = _velocity_for_path(schema_file)
            with_path_aliases(
                results["git_velocity"],
                schema_file,
                graph.graph.nodes[schema_file]["git_change_velocity"],
            )

        # Add dbt macro definitions as first-class module graph nodes.
        self._register_macro_nodes(graph, results, _velocity_for_path)
        self._register_ingestion_config_nodes(graph, _velocity_for_path)

        # --- Python file analysis via tree-sitter ---
        logger.info("Analyzing Python files with tree-sitter...")
        router = LanguageRouter(self.repo_path)
        modules, datasets, edges = router.analyze_directory(self.repo_path)

        # Add Python module nodes to the module graph
        for mod_node in modules:
            graph.add_node(mod_node)
            logger.info(f"[Surveyor] Added Python module: {mod_node.id}")
            if hasattr(mod_node, "source_file") and mod_node.source_file:
                repo_abs = os.path.abspath(self.repo_path)
                file_abs = os.path.abspath(os.path.join(self.repo_path, mod_node.source_file))
                file_in_repo = normalize_path_key(os.path.relpath(file_abs, repo_abs))
                velocity = _velocity_for_path(file_in_repo)
                graph.graph.nodes[mod_node.id]["git_change_velocity"] = velocity
                with_path_aliases(results["git_velocity"], file_in_repo, velocity)

        # Add Python datasets and edges to both graphs:
        # 1. To the module graph (direct access)
        for ds_node in datasets:
            graph.add_node(ds_node)
        for edge in edges:
            graph.add_edge(edge)

        # 2. To results for the hydrologist (lineage graph)
        results["datasets"].extend(datasets)
        results.setdefault("python_edges", []).extend(edges)

        logger.info(
            f"[Surveyor] Python analysis: {len(modules)} modules, "
            f"{len(datasets)} datasets, {len(edges)} edges"
        )

        return results
