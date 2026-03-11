import os
import logging
from ..analyzers.dag_config_parser import analyze_all_yaml_files
from ..analyzers.git_analyzer import get_git_change_velocity
from ..analyzers.tree_sitter_analyzer import LanguageRouter
from ..graph.knowledge_graph import KnowledgeGraph

logger = logging.getLogger(__name__)

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
            repo_abs = os.path.abspath(self.repo_path)
            file_abs = os.path.abspath(os.path.join(os.getcwd(), node.source_file))
            file_in_repo = os.path.relpath(file_abs, repo_abs)
            
            velocity = get_git_change_velocity(self.repo_path, file_in_repo)
            
            graph.add_node(node)
            graph.graph.nodes[node.id]["git_change_velocity"] = velocity

        # --- Python file analysis via tree-sitter ---
        print("[Surveyor] Analyzing Python files with tree-sitter...")
        router = LanguageRouter(self.repo_path)
        modules, datasets, edges = router.analyze_directory(self.repo_path)

        # Add Python module nodes to the module graph
        for mod_node in modules:
            graph.add_node(mod_node)
            logger.info(f"[Surveyor] Added Python module: {mod_node.id}")

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
