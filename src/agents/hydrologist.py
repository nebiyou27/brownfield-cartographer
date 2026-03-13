import os

from ..analyzers.sql_lineage import analyze_all_sql_files
from ..graph.knowledge_graph import KnowledgeGraph
from ..logger import get_logger
from ..models.schemas import TransformationEdge

logger = get_logger(__name__)


class Hydrologist:
    """
    Traces data flow through the SQL models and YAML configs,
    building a data lineage graph.
    """

    def __init__(self, repo_path: str):
        self.repo_path = repo_path

    def trace_lineage(
        self,
        graph: KnowledgeGraph,
        datasets: list,
        model_paths: list[str],
        macro_paths: list[str] | None = None,
        precomputed_edges: list[TransformationEdge] | None = None,
    ):
        logger.info("Tracing SQL data lineage in: %s...", model_paths)

        # Add nodes first (from the Surveyor's findings)
        for node in datasets:
            graph.add_node(node)

        # Include non-SQL lineage edges already extracted by other analyzers.
        if precomputed_edges:
            for edge in precomputed_edges:
                graph.add_edge(edge)

        # Extract and add edges for each configured model directory
        # Merge macros from ALL existing macro paths (not just the first)
        macros_dirs = []
        if macro_paths:
            for macro_path in macro_paths:
                candidate = os.path.join(self.repo_path, macro_path)
                if os.path.exists(candidate):
                    macros_dirs.append(candidate)

        for path in model_paths:
            models_dir = os.path.join(self.repo_path, path)
            if os.path.exists(models_dir):
                edges = analyze_all_sql_files(models_dir, macros_dirs=macros_dirs)
                for edge in edges:
                    graph.add_edge(edge)

                    # Also add the target as a node if it wasn't in YAML (just in case)
                    if edge.target_dataset not in graph.graph:
                        graph.graph.add_node(edge.target_dataset, id=edge.target_dataset)
                    if edge.source_dataset not in graph.graph:
                        graph.graph.add_node(edge.source_dataset, id=edge.source_dataset)
            else:
                logger.warning("Models directory not found at %s. Skipping.", models_dir)
