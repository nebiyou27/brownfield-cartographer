import os

from ..analyzers.dag_config_parser import analyze_all_dag_config_files
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

    @staticmethod
    def _merge_node_source(graph: KnowledgeGraph, node_id: str, source_label: str):
        node_data = graph.graph.nodes.get(node_id, {})
        sources = set(node_data.get("sources", []))
        sources.add(source_label)
        graph.graph.nodes[node_id]["sources"] = sorted(sources)

    def _add_or_merge_node(self, graph: KnowledgeGraph, node, source_label: str):
        if hasattr(node, "id"):
            node_id = node.id
            if node_id in graph.graph.nodes:
                self._merge_node_source(graph, node_id, source_label)
            else:
                graph.add_node(node)
                self._merge_node_source(graph, node_id, source_label)

    def _ensure_edge_nodes(
        self, graph: KnowledgeGraph, edge: TransformationEdge, source_label: str
    ):
        for node_id in (edge.source_dataset, edge.target_dataset):
            if node_id not in graph.graph:
                graph.graph.add_node(node_id, id=node_id, sources=[source_label])
            else:
                self._merge_node_source(graph, node_id, source_label)

    @staticmethod
    def _merge_edge(graph: KnowledgeGraph, edge: TransformationEdge, source_label: str):
        src, tgt = edge.source_dataset, edge.target_dataset
        edge_payload = edge.model_dump()
        if graph.graph.has_edge(src, tgt):
            existing = dict(graph.graph.get_edge_data(src, tgt) or {})
            variants = list(existing.get("edge_variants", []))
            if not variants:
                base = dict(existing)
                base.pop("edge_variants", None)
                base.pop("sources", None)
                variants.append(base)
            variants.append(edge_payload)
            merged_sources = set(existing.get("sources", []))
            merged_sources.add(source_label)
            existing["edge_variants"] = variants
            existing["sources"] = sorted(merged_sources)
            graph.graph.add_edge(src, tgt, **existing)
            return

        edge_payload["sources"] = [source_label]
        graph.graph.add_edge(src, tgt, **edge_payload)

    def trace_lineage(
        self,
        graph: KnowledgeGraph,
        datasets: list,
        model_paths: list[str],
        macro_paths: list[str] | None = None,
        precomputed_edges: list[TransformationEdge] | None = None,
        config_edges: list[TransformationEdge] | None = None,
    ):
        logger.info("Tracing SQL data lineage in: %s...", model_paths)

        # Add nodes first (from the Surveyor's findings)
        for node in datasets:
            self._add_or_merge_node(graph, node, source_label="config")

        # Include non-SQL lineage edges already extracted by other analyzers (Python).
        if precomputed_edges:
            for edge in precomputed_edges:
                self._ensure_edge_nodes(graph, edge, source_label="python")
                self._merge_edge(graph, edge, source_label="python")

        # Include lineage edges extracted from config declarations.
        if config_edges:
            for edge in config_edges:
                self._ensure_edge_nodes(graph, edge, source_label="config")
                self._merge_edge(graph, edge, source_label="config")

        detected_config_edges = analyze_all_dag_config_files(self.repo_path, model_paths)
        for edge in detected_config_edges:
            self._ensure_edge_nodes(graph, edge, source_label="config")
            self._merge_edge(graph, edge, source_label="config")

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
                    self._ensure_edge_nodes(graph, edge, source_label="sql")
                    self._merge_edge(graph, edge, source_label="sql")
            else:
                logger.warning("Models directory not found at %s. Skipping.", models_dir)
