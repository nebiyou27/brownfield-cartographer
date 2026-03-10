import os
from typing import List
from ..analyzers.sql_lineage import analyze_all_sql_files
from ..graph.knowledge_graph import KnowledgeGraph

class Hydrologist:
    """
    Traces data flow through the SQL models and YAML configs, 
    building a data lineage graph.
    """
    def __init__(self, repo_path: str):
        self.repo_path = repo_path

    def trace_lineage(self, graph: KnowledgeGraph, datasets: list, model_paths: List[str]):
        print(f"[Hydrologist] Tracing SQL data lineage in: {model_paths}...")
        
        # Add nodes first (from the Surveyor's findings)
        for node in datasets:
            graph.add_node(node)
            
        # Extract and add edges for each configured model directory
        for path in model_paths:
            models_dir = os.path.join(self.repo_path, path)
            if os.path.exists(models_dir):
                edges = analyze_all_sql_files(models_dir)
                for edge in edges:
                    graph.add_edge(edge)
                    
                    # Also add the target as a node if it wasn't in YAML (just in case)
                    if edge.target_dataset not in graph.graph:
                        graph.graph.add_node(edge.target_dataset, id=edge.target_dataset)
                    if edge.source_dataset not in graph.graph:
                        graph.graph.add_node(edge.source_dataset, id=edge.source_dataset)
            else:
                print(f"[WARNING] Models directory not found at {models_dir}. Skipping.")
