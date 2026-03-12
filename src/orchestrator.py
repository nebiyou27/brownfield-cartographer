import json
import os
from .agents.surveyor import Surveyor
from .agents.hydrologist import Hydrologist
from .agents.semanticist import Semanticist
from .graph.knowledge_graph import KnowledgeGraph


class Orchestrator:
    """
    The brain of the system.
    Coordinates the Surveyor → Hydrologist → Semanticist agents
    to produce the final output files.
    """

    def __init__(self, target_dir: str, skip_semanticist: bool = False):
        self.target_dir = target_dir
        self.skip_semanticist = skip_semanticist
        # Output folder for our results
        self.output_dir = os.path.join(os.getcwd(), ".cartography")

        # Initialize our two target graphs
        self.module_graph = KnowledgeGraph("Module Graph")
        self.lineage_graph = KnowledgeGraph("Lineage Graph")

        # Initialize agents
        self.surveyor = Surveyor(self.target_dir)
        self.hydrologist = Hydrologist(self.target_dir)
        self.semanticist = Semanticist(self.target_dir)

    def run_analysis(self):
        """
        Runs the full analysis pipeline on the target repository.
        """
        print(f"\n--- Starting Analysis on {self.target_dir} ---")

        # 1. Surveyor — identifies components and project structure
        survey_results = self.surveyor.survey(self.module_graph)

        # 2. Hydrologist — traces data lineage
        model_paths = survey_results.get("model_paths", ["models"])
        macro_paths = survey_results.get("macro_paths", ["macros"])
        self.hydrologist.trace_lineage(
            self.lineage_graph,
            survey_results["datasets"],
            model_paths,
            macro_paths=macro_paths,
            precomputed_edges=survey_results.get("python_edges", []),
        )

        # 3. Semanticist — LLM-powered purpose extraction, drift, clustering,
        #    and Day-One synthesis. Can be skipped with --no-semanticist flag
        #    (useful for fast iteration during development).
        semantic_results = {}
        if self.skip_semanticist:
            print("\n[Semanticist] Skipped (--no-semanticist flag set)")
        else:
            try:
                semantic_results = self.semanticist.analyse(
                    self.lineage_graph,
                    module_graph=self.module_graph,
                    git_velocity=survey_results.get("git_velocity", {}),
                )
                self._save_semantic_results(semantic_results)
            except Exception as e:
                print(f"\n[WARNING] Semanticist failed: {e}")
                print("[WARNING] Continuing without semantic enrichment.")

        # 4. Finalize and save graphs
        print("\n--- Finalizing Graphs ---")

        orphaned_nodes = self.find_orphaned_nodes(self.lineage_graph.graph)
        for node_id in orphaned_nodes:
            print(f"[WARNING] Orphaned node detected: {node_id}")
            self.lineage_graph.graph.nodes[node_id]["orphaned"] = True

        self.module_graph.save_json(
            os.path.join(self.output_dir, "module_graph.json")
        )
        self.lineage_graph.save_json(
            os.path.join(self.output_dir, "lineage_graph.json")
        )

        # 5. Parse-failure diagnostics
        failed_nodes = [
            (nid, attrs)
            for nid, attrs in self.lineage_graph.graph.nodes(data=True)
            if attrs.get("parsed") is False
        ]
        if failed_nodes:
            print(f"\n--- Parse-Failure Diagnostics ({len(failed_nodes)} file(s)) ---")
            for nid, attrs in failed_nodes:
                reason = attrs.get("reason", "unknown")
                print(f"  FAIL: {nid}")
                print(f"        Reason: {reason}")
            print("---")
        else:
            print("\n[OK] All files parsed successfully — no parse failures.")

        # 6. Export human-readable lineage
        lineage_text = self.lineage_graph.export_lineage_text()
        with open("lineage_final.txt", "w", encoding="utf-8") as f:
            f.write(f"Testing analyzer on {self.target_dir}...\n")
            f.write(lineage_text)
        print("[SUCCESS] Human-readable lineage exported to lineage_final.txt")

        print(f"\n[SUCCESS] Analysis complete! Results saved to {self.output_dir}")

    # ------------------------------------------------------------------
    def _save_semantic_results(self, results: dict):
        """Persist all Semanticist outputs to .cartography/"""
        os.makedirs(self.output_dir, exist_ok=True)

        # purpose_statements.json
        purpose_path = os.path.join(self.output_dir, "purpose_statements.json")
        with open(purpose_path, "w", encoding="utf-8") as f:
            json.dump(results.get("purpose_statements", {}), f, indent=2)
        print(f"[Semanticist] Saved purpose statements → {purpose_path}")

        # drift_flags.json
        drift_path = os.path.join(self.output_dir, "drift_flags.json")
        with open(drift_path, "w", encoding="utf-8") as f:
            json.dump(results.get("drift_flags", {}), f, indent=2)
        print(f"[Semanticist] Saved drift flags → {drift_path}")

        # domain_map.json
        domain_path = os.path.join(self.output_dir, "domain_map.json")
        with open(domain_path, "w", encoding="utf-8") as f:
            json.dump(results.get("domain_map", {}), f, indent=2)
        print(f"[Semanticist] Saved domain map → {domain_path}")

        # onboarding_brief.md  (Day-One answers)
        brief_path = os.path.join(self.output_dir, "onboarding_brief.md")
        day_one = results.get("day_one_answers", "")
        with open(brief_path, "w", encoding="utf-8") as f:
            f.write("# FDE Day-One Onboarding Brief\n\n")
            f.write(day_one)
        print(f"[Semanticist] Saved onboarding brief → {brief_path}")

        # budget_summary.json
        budget_path = os.path.join(self.output_dir, "budget_summary.json")
        with open(budget_path, "w", encoding="utf-8") as f:
            json.dump(results.get("budget_summary", {}), f, indent=2)

    # ------------------------------------------------------------------
    def find_orphaned_nodes(self, graph) -> list:
        """Identifies nodes that have no incoming or outgoing edges."""
        all_nodes = set(graph.nodes())
        nodes_with_edges = set()
        for u, v in graph.edges():
            nodes_with_edges.add(u)
            nodes_with_edges.add(v)
        return list(all_nodes - nodes_with_edges)