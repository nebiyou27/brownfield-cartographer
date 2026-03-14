import json
import os

import networkx as nx

from ..logger import get_logger
from ..models.schemas import DatasetNode, ModuleNode, TransformationEdge

logger = get_logger(__name__)


class KnowledgeGraph:
    """
    Wraps networkx DiGraph to manage and serialize our codebase maps.
    Handles both the Module Graph and the Data Lineage Graph.
    """

    def __init__(self, name: str):
        self.name = name
        self.graph = nx.DiGraph()

    def add_node(self, node: ModuleNode | DatasetNode):
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
            - path_confidence: minimum confidence across the shortest path
            - reason: confidence reason from the weakest edge on that path
        """
        if node not in self.graph:
            return []

        descendants = nx.descendants(self.graph, node)
        if not descendants:
            return []

        distances = dict(nx.single_source_shortest_path_length(self.graph, node))
        distances.pop(node, None)

        results: list[dict] = []
        for target, _distance in sorted(distances.items(), key=lambda item: (item[1], item[0])):
            path = nx.shortest_path(self.graph, node, target)
            min_conf = 1.0
            weakest_reason = "no reason recorded"
            for path_u, path_v in zip(path, path[1:], strict=False):
                edge_data = self.graph.get_edge_data(path_u, path_v) or {}
                conf_value = edge_data.get("confidence", 1.0)
                try:
                    conf = float(conf_value)
                except (TypeError, ValueError):
                    conf = 1.0
                conf = max(0.0, min(1.0, conf))
                if conf <= min_conf:
                    min_conf = conf
                    weakest_reason = edge_data.get("confidence_reason", "no reason recorded")

            results.append(
                {
                    "node": target,
                    "path_confidence": min_conf,
                    "reason": weakest_reason,
                }
            )

        return results
