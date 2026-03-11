import os
from .agents.surveyor import Surveyor
from .agents.hydrologist import Hydrologist
from .graph.knowledge_graph import KnowledgeGraph

class Orchestrator:
    """
    The brain of the system. 
    Coordinates the Surveyor and Hydrologist agents to produce the final output files.
    """
    def __init__(self, target_dir: str):
        self.target_dir = target_dir
        # Output folder for our results
        self.output_dir = os.path.join(os.getcwd(), ".cartography")
        
        # Initialize our two target graphs
        self.module_graph = KnowledgeGraph("Module Graph")
        self.lineage_graph = KnowledgeGraph("Lineage Graph")
        
        # Initialize our two agents
        self.surveyor = Surveyor(self.target_dir)
        self.hydrologist = Hydrologist(self.target_dir)

    def run_analysis(self):
        """
        Runs the full analysis pipeline on the target repository.
        """
        print(f"\n--- Starting Analysis on {self.target_dir} ---")
        
        # 1. Surveyor identifies the components and project structure
        survey_results = self.surveyor.survey(self.module_graph)
        
        # 2. Hydrologist traces the data lineage based on the identified components
        # We use the model paths resolved by the Surveyor
        model_paths = survey_results.get("model_paths", ["models"])
        macro_paths = survey_results.get("macro_paths", ["macros"])
        self.hydrologist.trace_lineage(
            self.lineage_graph,
            survey_results["datasets"],
            model_paths,
            macro_paths=macro_paths,
            precomputed_edges=survey_results.get("python_edges", []),
        )

        # 3. Finalize and save
        print("\n--- Finalizing Graphs ---")
        
        # Detect orphaned nodes in the lineage graph
        orphaned_nodes = self.find_orphaned_nodes(self.lineage_graph.graph)
        for node_id in orphaned_nodes:
            print(f"[WARNING] Orphaned node detected: {node_id}")
            self.lineage_graph.graph.nodes[node_id]["orphaned"] = True

        self.module_graph.save_json(os.path.join(self.output_dir, "module_graph.json"))
        self.lineage_graph.save_json(os.path.join(self.output_dir, "lineage_graph.json"))
        
        # 4. Export human-readable lineage for the final submission
        lineage_text = self.lineage_graph.export_lineage_text()
        with open("lineage_final.txt", "w", encoding="utf-8") as f:
            f.write(f"Testing analyzer on {self.target_dir}...\n")
            f.write(lineage_text)
        print("[SUCCESS] Human-readable lineage exported to lineage_final.txt")
        
        print(f"\n[SUCCESS] Analysis complete! Results saved to {self.output_dir}")

    def find_orphaned_nodes(self, graph) -> list:
        """
        Identifies nodes that have no incoming or outgoing edges.
        """
        all_nodes = set(graph.nodes())
        nodes_with_edges = set()
        for u, v in graph.edges():
            nodes_with_edges.add(u)
            nodes_with_edges.add(v)
        
        orphaned = list(all_nodes - nodes_with_edges)
        return orphaned
