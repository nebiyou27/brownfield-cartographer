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
        self.hydrologist.trace_lineage(self.lineage_graph, survey_results["datasets"], model_paths)

        # 3. Finalize and save
        print("\n--- Finalizing Graphs ---")
        self.module_graph.save_json(os.path.join(self.output_dir, "module_graph.json"))
        self.lineage_graph.save_json(os.path.join(self.output_dir, "lineage_graph.json"))
        
        print(f"\n[SUCCESS] Analysis complete! Results saved to {self.output_dir}")
