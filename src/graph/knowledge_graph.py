import json
import os
import networkx as nx
from typing import List, Union
from ..models.schemas import ModuleNode, DatasetNode, TransformationEdge

class KnowledgeGraph:
    """
    Wraps networkx DiGraph to manage and serialize our codebase maps.
    Handles both the Module Graph and the Data Lineage Graph.
    """
    def __init__(self, name: str):
        self.name = name
        self.graph = nx.DiGraph()

    def add_node(self, node: Union[ModuleNode, DatasetNode]):
        """
        Adds a node to the graph using its Pydantic schema data.
        """
        self.graph.add_node(
            node.id, 
            **node.model_dump() # Stores all pydantic fields as node attributes
        )

    def add_edge(self, edge: TransformationEdge):
        """
        Adds a directional edge between datasets.
        """
        self.graph.add_edge(
            edge.source_dataset, 
            edge.target_dataset, 
            **edge.model_dump()
        )

    def save_json(self, output_path: str):
        """
        Serializes the graph to a JSON file that can be read by other tools.
        """
        # Ensure the directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # networkx provides an adjacency representation that is easy to serialize
        data = nx.node_link_data(self.graph)
        
        # Ensure 'edges' key is used instead of 'links' for compatibility with other tools
        if 'links' in data:
            data['edges'] = data.pop('links')
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            print(f"[Graph] Saved {self.name} to {output_path}")
        except Exception as e:
            print(f"[ERROR] Failed to save graph to {output_path}: {e}")

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
