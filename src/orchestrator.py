import json
import os
import warnings

from .agents.surveyor import Surveyor
from .agents.hydrologist import Hydrologist
from .agents.semanticist import Semanticist
from .agents.archivist import Archivist
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
        self.output_dir = os.path.join(os.getcwd(), ".cartography")

        self.module_graph = KnowledgeGraph("Module Graph")
        self.lineage_graph = KnowledgeGraph("Lineage Graph")

        self.surveyor = Surveyor(self.target_dir)
        self.hydrologist = Hydrologist(self.target_dir)
        self.semanticist = Semanticist(self.target_dir)
        self.archivist = Archivist(self.target_dir)

    def run_analysis(self):
        print(f"\n--- Starting Analysis on {self.target_dir} ---")

        # 1. Surveyor — structural analysis + git velocity
        survey_results = self.surveyor.survey(self.module_graph)

        # 2. Hydrologist — data lineage
        model_paths = survey_results.get("model_paths", ["models"])
        macro_paths = survey_results.get("macro_paths", ["macros"])
        self.hydrologist.trace_lineage(
            self.lineage_graph,
            survey_results["datasets"],
            model_paths,
            macro_paths=macro_paths,
            precomputed_edges=survey_results.get("python_edges", []),
        )

        # 3. Semanticist — purpose statements, drift, clustering, Day-One answers.
        #    git_velocity from Surveyor grounds Q5 in real git data rather than
        #    heuristic guesses from file naming conventions.
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

                # 4. Archivist — produce CODEBASE.md and audit_trace.log
                self.archivist.archive(
                    self.module_graph,
                    self.lineage_graph,
                    semantic_results,
                    git_velocity=survey_results.get("git_velocity", {}),
                )
            except Exception as e:
                print(f"\n[WARNING] Semanticist failed: {e}")
                print("[WARNING] Continuing without semantic enrichment.")

        # 4. Finalize graphs
        print("\n--- Finalizing Graphs ---")

        orphaned_nodes = self.find_orphaned_nodes(self.lineage_graph.graph)
        for node_id in orphaned_nodes:
            print(f"[WARNING] Orphaned node detected: {node_id}")
            self.lineage_graph.graph.nodes[node_id]["orphaned"] = True

        # Suppress NetworkX FutureWarning on node_link_data edges kwarg
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
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
                print(f"  FAIL: {nid}")
                print(f"        Reason: {attrs.get('reason', 'unknown')}")
            print("---")
        else:
            print("\n[OK] All files parsed successfully — no parse failures.")

        # 6. Human-readable lineage export
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

        files = {
            "purpose_statements.json": results.get("purpose_statements", {}),
            "drift_flags.json":        results.get("drift_flags", {}),
            "domain_map.json":         results.get("domain_map", {}),
            "budget_summary.json":     results.get("budget_summary", {}),
        }
        for filename, data in files.items():
            path = os.path.join(self.output_dir, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"[Semanticist] Saved {filename} → {path}")

        # onboarding_brief.md — plain text, not JSON
        brief_path = os.path.join(self.output_dir, "onboarding_brief.md")
        with open(brief_path, "w", encoding="utf-8") as f:
            f.write("# FDE Day-One Onboarding Brief\n\n")
            f.write(results.get("day_one_answers", ""))
        print(f"[Semanticist] Saved onboarding_brief.md → {brief_path}")

    # ------------------------------------------------------------------
    def find_orphaned_nodes(self, graph) -> list:
        """Identifies nodes with no incoming or outgoing edges."""
        nodes_with_edges = {n for u, v in graph.edges() for n in (u, v)}
        return list(set(graph.nodes()) - nodes_with_edges)