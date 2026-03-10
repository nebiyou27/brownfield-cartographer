import os
from ..analyzers.dag_config_parser import analyze_all_yaml_files
from ..analyzers.git_analyzer import get_git_change_velocity
from ..graph.knowledge_graph import KnowledgeGraph

class Surveyor:
    """
    Scans the repository, builds a module/file dependency graph, 
    identifies which files import which, and computes git change velocity.
    """
    def __init__(self, repo_path: str):
        self.repo_path = repo_path

    def survey(self, graph: KnowledgeGraph):
        print("[Surveyor] Scanning project structure and YAML configs...")
        results = analyze_all_yaml_files(self.repo_path)
        
        # Add the project node
        if results["project"]:
            graph.add_node(results["project"])
            
        # Add all datasets (models/sources) as modules
        for node in results["datasets"]:
            # node.source_file is relative to the PROJECT root (e.g. jaffle_shop/models/schema.yml)
            # We need to find its path relative to the REPO root (e.g. models/schema.yml)
            # since we run git commands inside self.repo_path.
            
            repo_abs = os.path.abspath(self.repo_path)
            file_abs = os.path.abspath(os.path.join(os.getcwd(), node.source_file))
            file_in_repo = os.path.relpath(file_abs, repo_abs)
            
            velocity = get_git_change_velocity(self.repo_path, file_in_repo)
            
            graph.add_node(node)
            graph.graph.nodes[node.id]["git_change_velocity"] = velocity
            
        return results
