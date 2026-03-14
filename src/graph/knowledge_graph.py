import json
import os

import networkx as nx

from ..logger import get_logger
from ..models.schemas import DatasetNode, MacroNode, ModuleNode, TransformationEdge

logger = get_logger(__name__)


class KnowledgeGraph:
    """
    Wraps networkx DiGraph to manage and serialize our codebase maps.
    Handles both the Module Graph and the Data Lineage Graph.
    """

    def __init__(self, name: str):
        self.name = name
        self.graph = nx.DiGraph()

    def add_node(self, node: ModuleNode | DatasetNode | MacroNode):
        """
        Adds a node to the graph using its Pydantic schema data.
        """
        self.graph.add_node(
            node.id,
            **node.model_dump(),  # Stores all pydantic fields as node attributes
        )

    def add_edge(self, edge: TransformationEdge):
        """
        Adds a directional edge between datasets.
        """
        self.graph.add_edge(edge.source_dataset, edge.target_dataset, **edge.model_dump())

    def save_json(self, output_path: str):
        """
        Serializes the graph to a JSON file that can be read by other tools.
        """
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # networkx provides an adjacency representation that is easy to serialize
        data = nx.node_link_data(self.graph)

        # Ensure 'edges' key is used instead of 'links' for compatibility with other tools
        if "links" in data:
            data["edges"] = data.pop("links")

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            logger.info("Saved %s to %s", self.name, output_path)
        except Exception as e:
            logger.error("Failed to save graph to %s: %s", output_path, e)

    def export_lineage_text(self) -> str:
        """
        Returns a human-readable text representation of the data lineage.
        """
        lines = ["=== Extracted Lineage ==="]
        # Use networkx edges to build the lineage list
        for source, target, data in self.graph.edges(data=True):
            source_file = data.get("source_file", "unknown")
            lines.append(f"{source} -> {target} (from {source_file})")
        return "\n".join(lines)

    def find_sources(self) -> list:
        """
        Return nodes with in-degree 0 in the unified merged lineage graph
        combining SQL, Python, and config lineage.
        """
        return [node for node, degree in self.graph.in_degree() if degree == 0]

    def find_sinks(self) -> list:
        """
        Return nodes with out-degree 0 in the unified merged lineage graph
        combining SQL, Python, and config lineage.
        """
        return [node for node, degree in self.graph.out_degree() if degree == 0]

    def blast_radius(self, node: str) -> list[dict]:
        """
        Return downstream impact in the unified merged lineage graph combining SQL,
        Python, and config lineage.

        For each downstream node, returns:
            - node: downstream node id
            - path_confidence: conservative confidence across all shortest paths
            - reason: confidence reason from the weakest edge on the selected path
            - uncertain: True when confidence metadata was missing on any weakest edge
            - unknown_confidence_edges: count of edges on the selected path without explicit confidence
        """
        if node not in self.graph:
            return []

        descendants = nx.descendants(self.graph, node)
        if not descendants:
            return []

        distances = dict(nx.single_source_shortest_path_length(self.graph, node))
        distances.pop(node, None)

        def _parse_confidence(edge_data: dict) -> tuple[float, bool]:
            """
            Return (confidence, is_unknown).
            Unknown values are treated as medium confidence to avoid false certainty.
            """
            if "confidence" not in edge_data:
                return 0.50, True
            conf_value = edge_data.get("confidence")
            try:
                conf = float(conf_value)
            except (TypeError, ValueError):
                return 0.50, True
            return max(0.0, min(1.0, conf)), False

        results: list[dict] = []
        for target, _distance in sorted(distances.items(), key=lambda item: (item[1], item[0])):
            shortest_paths = list(nx.all_shortest_paths(self.graph, node, target))

            selected_path = shortest_paths[0]
            selected_min_conf = 1.0
            selected_reason = "no reason recorded"
            selected_unknown_edges = 0

            for path in shortest_paths:
                path_min_conf = 1.0
                weakest_reason = "no reason recorded"
                unknown_edges = 0

                for path_u, path_v in zip(path, path[1:], strict=False):
                    edge_data = self.graph.get_edge_data(path_u, path_v) or {}
                    conf, is_unknown = _parse_confidence(edge_data)
                    if is_unknown:
                        unknown_edges += 1
                    if conf <= path_min_conf:
                        path_min_conf = conf
                        weakest_reason = edge_data.get(
                            "confidence_reason",
                            "confidence missing; treated as uncertain",
                        )

                if path_min_conf < selected_min_conf or (
                    path_min_conf == selected_min_conf and unknown_edges > selected_unknown_edges
                ):
                    selected_path = path
                    selected_min_conf = path_min_conf
                    selected_reason = weakest_reason
                    selected_unknown_edges = unknown_edges

            results.append(
                {
                    "node": target,
                    "path_confidence": selected_min_conf,
                    "reason": selected_reason,
                    "uncertain": selected_unknown_edges > 0,
                    "unknown_confidence_edges": selected_unknown_edges,
                    "path": selected_path,
                }
            )

        return results
