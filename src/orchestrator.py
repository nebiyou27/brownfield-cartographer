import json
import os
import subprocess
import warnings
from collections import Counter

import networkx as nx

from .agents.archivist import Archivist
from .agents.hydrologist import Hydrologist
from .agents.semanticist import Semanticist
from .agents.surveyor import Surveyor
from .analyzers.dag_config_parser import analyze_all_yaml_files
from .analyzers.git_analyzer import get_git_change_velocity
from .analyzers.sql_lineage import analyze_sql_file
from .analyzers.tree_sitter_analyzer import LanguageRouter
from .graph.knowledge_graph import KnowledgeGraph
from .logger import get_logger
from .path_utils import normalize_path_key, with_path_aliases

logger = get_logger(__name__)


class Orchestrator:
    """
    The brain of the system.
    Coordinates the Surveyor → Hydrologist → Semanticist agents
    to produce the final output files.
    """

    def __init__(self, target_dir: str, skip_semanticist: bool = False, incremental: bool = False):
        self.target_dir = target_dir
        self.skip_semanticist = skip_semanticist
        self.incremental = incremental
        self.output_dir = os.path.join(os.getcwd(), ".cartography")

        self.module_graph = KnowledgeGraph("Module Graph")
        self.lineage_graph = KnowledgeGraph("Lineage Graph")

        self.surveyor = Surveyor(self.target_dir)
        self.hydrologist = Hydrologist(self.target_dir)
        self.semanticist = Semanticist(self.target_dir)
        self.archivist = Archivist(self.target_dir)

    def run_analysis(self):
        logger.info("--- Starting Analysis on %s ---", self.target_dir)
        os.makedirs(self.output_dir, exist_ok=True)

        if self.incremental and self._run_incremental_if_possible():
            return

        # 1. Surveyor — structural analysis + git velocity
        try:
            survey_results = self.surveyor.survey(self.module_graph)
            self.module_graph.save_json(os.path.join(self.output_dir, "module_graph.json"))
        except Exception as e:
            logger.error("Surveyor failed: %s", e)
            return

        # 2. Hydrologist — data lineage
        try:
            model_paths = survey_results.get("model_paths", ["models"])
            macro_paths = survey_results.get("macro_paths", ["macros"])
            self.hydrologist.trace_lineage(
                self.lineage_graph,
                survey_results["datasets"],
                model_paths,
                macro_paths=macro_paths,
                precomputed_edges=survey_results.get("python_edges", []),
            )
            self.lineage_graph.save_json(os.path.join(self.output_dir, "lineage_graph.json"))
        except Exception as e:
            logger.error("Hydrologist failed: %s", e)
            # We continue because Surveyor results might still be useful

        # 3. Semanticist — purpose statements, drift, clustering, Day-One answers.
        semantic_results = {}
        graph_intelligence = self._build_graph_intelligence(
            self.lineage_graph,
            self.module_graph,
            survey_results.get("git_velocity", {}),
        )
        if self.skip_semanticist:
            logger.info("[Semanticist] Skipped (--no-semanticist flag set)")
        else:
            try:
                semantic_results = self.semanticist.analyse(
                    self.lineage_graph,
                    module_graph=self.module_graph,
                    git_velocity=survey_results.get("git_velocity", {}),
                    graph_intelligence=graph_intelligence,
                )
                self._save_semantic_results(semantic_results)
            except Exception as e:
                logger.warning("[WARNING] Semanticist failed: %s", e)
                logger.warning("[WARNING] Continuing without semantic enrichment.")

        # 4. Finalize artifacts
        logger.info("--- Finalizing Artifacts ---")

        orphaned_nodes = self.find_orphaned_nodes(self.lineage_graph.graph)
        for node_id in orphaned_nodes:
            logger.warning("Orphaned node detected: %s", node_id)
            self.lineage_graph.graph.nodes[node_id]["orphaned"] = True

        # 5. Archivist — produce CODEBASE.md and audit_trace.log
        try:
            self.archivist.archive(
                self.module_graph,
                self.lineage_graph,
                semantic_results,
                git_velocity=survey_results.get("git_velocity", {}),
            )
        except Exception as e:
            logger.error("Archivist failed: %s", e)

        # Suppress NetworkX FutureWarning on node_link_data edges kwarg
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            # Standard saves (redundant but ensures the latest state is captured)
            self.module_graph.save_json(os.path.join(self.output_dir, "module_graph.json"))
            self.lineage_graph.save_json(os.path.join(self.output_dir, "lineage_graph.json"))

        # 6. Parse-failure diagnostics
        failed_nodes = [
            (nid, attrs)
            for nid, attrs in self.lineage_graph.graph.nodes(data=True)
            if attrs.get("parsed") is False
        ]
        if failed_nodes:
            logger.info("--- Parse-Failure Diagnostics (%d file(s)) ---", len(failed_nodes))
            for nid, attrs in failed_nodes:
                logger.info("  FAIL: %s", nid)
                logger.info("        Reason: %s", attrs.get("reason", "unknown"))
            logger.info("---")
        else:
            logger.info("All files parsed successfully — no parse failures.")

        lineage_text = self.lineage_graph.export_lineage_text()
        with open("lineage_final.txt", "w", encoding="utf-8") as f:
            f.write(f"Testing analyzer on {self.target_dir}...\n")
            f.write(lineage_text)
        logger.info("Human-readable lineage exported to lineage_final.txt")

        logger.info("Analysis complete! Results saved to %s", self.output_dir)
        if self.incremental:
            self._persist_incremental_state(self._get_current_commit_hash())

    # ------------------------------------------------------------------
    def _save_semantic_results(self, results: dict):
        """Persist all Semanticist outputs to .cartography/"""
        os.makedirs(self.output_dir, exist_ok=True)

        files = {
            "purpose_statements.json": results.get("purpose_statements", {}),
            "drift_flags.json": results.get("drift_flags", {}),
            "domain_map.json": results.get("domain_map", {}),
            "budget_summary.json": results.get("budget_summary", {}),
        }
        for filename, data in files.items():
            path = os.path.join(self.output_dir, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info("[Semanticist] Saved %s → %s", filename, path)

        # onboarding_brief.md — plain text, not JSON
        brief_path = os.path.join(self.output_dir, "onboarding_brief.md")
        with open(brief_path, "w", encoding="utf-8") as f:
            f.write("# FDE Day-One Onboarding Brief\n\n")
            f.write(results.get("day_one_answers", ""))
        logger.info("[Semanticist] Saved onboarding_brief.md → %s", brief_path)

    # ------------------------------------------------------------------
    def find_orphaned_nodes(self, graph) -> list:
        """Identifies nodes with no incoming or outgoing edges."""
        nodes_with_edges = {n for u, v in graph.edges() for n in (u, v)}
        return list(set(graph.nodes()) - nodes_with_edges)

    def _build_graph_intelligence(
        self,
        lineage_graph: KnowledgeGraph,
        module_graph: KnowledgeGraph,
        git_velocity: dict[str, int],
    ) -> dict:
        intelligence: dict = {
            "critical_nodes": [],
            "true_sources": [],
            "true_sinks": [],
            "blast_radius_top5": {},
            "high_velocity_files": [],
            "ingestion_pipeline": [],
            "cross_domain_risk": [],
            "macro_summary": {},
        }

        try:
            pagerank_scores = nx.pagerank(lineage_graph.graph, alpha=0.85)
        except Exception:
            pagerank_scores = {}

        ranked_nodes = sorted(pagerank_scores.items(), key=lambda item: item[1], reverse=True)[:10]
        for node_id, score in ranked_nodes:
            intelligence["critical_nodes"].append(
                {
                    "node": node_id,
                    "score": float(score),
                    "in_degree": int(lineage_graph.graph.in_degree(node_id)),
                    "out_degree": int(lineage_graph.graph.out_degree(node_id)),
                }
            )

        intelligence["true_sources"] = sorted(lineage_graph.find_sources())
        intelligence["true_sinks"] = sorted(lineage_graph.find_sinks())

        for item in intelligence["critical_nodes"][:5]:
            node_id = item["node"]
            intelligence["blast_radius_top5"][node_id] = lineage_graph.blast_radius(node_id)

        target_domain_tokens = {
            "recensement",
            "geographie",
            "foncier",
            "sante",
            "revenu",
            "emploi",
            "datatourisme",
            "risques",
        }

        def _extract_domain_tokens(value: str) -> set[str]:
            normalized = normalize_path_key(value).lower()
            tokens = set(normalized.split("/"))
            return {token for token in target_domain_tokens if token in tokens}

        def _domains_from_path_edges(path_nodes: list[str]) -> set[str]:
            domains: set[str] = set()
            for path_u, path_v in zip(path_nodes, path_nodes[1:], strict=False):
                edge_data = lineage_graph.graph.get_edge_data(path_u, path_v) or {}
                source_file = edge_data.get("source_file")
                if isinstance(source_file, str):
                    domains.update(_extract_domain_tokens(source_file))
            return domains

        def _domains_for_lineage_identity(node_id: str) -> set[str]:
            domains = _extract_domain_tokens(str(node_id))
            if node_id in lineage_graph.graph:
                source_file = lineage_graph.graph.nodes[node_id].get("source_file")
                if isinstance(source_file, str):
                    domains.update(_extract_domain_tokens(source_file))
            if node_id in module_graph.graph:
                source_file = module_graph.graph.nodes[node_id].get("source_file")
                if isinstance(source_file, str):
                    domains.update(_extract_domain_tokens(source_file))
            return domains

        def _map_domain_from_path(path: str) -> str | None:
            normalized = normalize_path_key(path).lower()
            tokens = normalized.split("/")
            mapping = {
                "recensement": "census",
                "census": "census",
                "geographie": "geography",
                "geography": "geography",
                "geo": "geography",
                "revenu": "income",
                "income": "income",
                "sante": "health",
                "health": "health",
                "foncier": "real_estate",
                "immobilier": "real_estate",
                "real_estate": "real_estate",
                "dvf": "real_estate",
            }
            for token in tokens:
                if token in mapping:
                    return mapping[token]
            return None

        def _domains_for_node(node_id: str) -> set[str]:
            domains: set[str] = set()
            if node_id in module_graph.graph:
                source_file = module_graph.graph.nodes[node_id].get("source_file")
                if isinstance(source_file, str):
                    mapped = _map_domain_from_path(source_file)
                    if mapped:
                        domains.add(mapped)
            for src, _tgt, edge_data in lineage_graph.graph.in_edges(node_id, data=True):
                source_file = edge_data.get("source_file")
                if isinstance(source_file, str):
                    mapped = _map_domain_from_path(source_file)
                    if mapped:
                        domains.add(mapped)
                if src in module_graph.graph:
                    src_file = module_graph.graph.nodes[src].get("source_file")
                    if isinstance(src_file, str):
                        mapped = _map_domain_from_path(src_file)
                        if mapped:
                            domains.add(mapped)
            for _src, _tgt, edge_data in lineage_graph.graph.out_edges(node_id, data=True):
                source_file = edge_data.get("source_file")
                if isinstance(source_file, str):
                    mapped = _map_domain_from_path(source_file)
                    if mapped:
                        domains.add(mapped)
            return domains

        cross_domain_risk: list[dict[str, object]] = []
        node_scores = {
            item["node"]: float(item.get("score", 0.0)) for item in intelligence["critical_nodes"]
        }
        for node_id in lineage_graph.graph.nodes():
            impacted = lineage_graph.blast_radius(node_id)
            if not impacted:
                continue

            domains = _domains_for_lineage_identity(str(node_id))
            direct_domains: set[str] = set()
            for _src, direct_tgt, edge_data in lineage_graph.graph.out_edges(node_id, data=True):
                if isinstance(direct_tgt, str):
                    direct_domains.update(_domains_for_lineage_identity(direct_tgt))
                source_file = edge_data.get("source_file")
                if isinstance(source_file, str):
                    direct_domains.update(_extract_domain_tokens(source_file))
            for impacted_item in impacted:
                impacted_node = impacted_item.get("node")
                if isinstance(impacted_node, str):
                    domains.update(_domains_for_lineage_identity(impacted_node))
                path_nodes = impacted_item.get("path")
                if isinstance(path_nodes, list):
                    domains.update(
                        _domains_from_path_edges([str(path_node) for path_node in path_nodes])
                    )

            if not domains:
                domains = _domains_for_node(node_id)
                for impacted_item in impacted:
                    impacted_node = impacted_item.get("node")
                    if isinstance(impacted_node, str):
                        domains.update(_domains_for_node(impacted_node))

            if len(domains) >= 3:
                cross_domain_risk.append(
                    {
                        "node": node_id,
                        "domains": sorted(domains),
                        "domain_count": len(domains),
                        "direct_domain_count": len(direct_domains),
                        "downstream_node_count": len(impacted),
                        "blast_radius_size": len(impacted),
                        "score": float(node_scores.get(node_id, 0.0)),
                    }
                )
        intelligence["cross_domain_risk"] = sorted(
            cross_domain_risk,
            key=lambda item: (
                -int(item["domain_count"]),
                -int(item.get("direct_domain_count", 0)),
                -int(item["downstream_node_count"]),
                -float(item.get("score", 0.0)),
                item["node"],
            ),
        )[:3]

        velocity_aliases: dict[str, int] = {}
        canonical_velocity: dict[str, int] = {}
        for path, commits in (git_velocity or {}).items():
            if isinstance(path, str) and isinstance(commits, int):
                with_path_aliases(velocity_aliases, path, commits)
                canonical = normalize_path_key(path)
                previous = canonical_velocity.get(canonical, 0)
                canonical_velocity[canonical] = max(previous, commits)

        # Backfill missing velocity on non-Python file nodes so SQL/YAML/CSV nodes
        # remain available to prompt sections and downstream reports.
        for _node_id, attrs in module_graph.graph.nodes(data=True):
            source_file = attrs.get("source_file")
            if not isinstance(source_file, str) or not source_file:
                continue
            suffix = os.path.splitext(source_file)[1].lower()
            if suffix not in (".sql", ".yml", ".yaml", ".csv"):
                continue
            existing = attrs.get("git_change_velocity")
            if isinstance(existing, int) and existing > 0:
                continue
            resolved = velocity_aliases.get(normalize_path_key(source_file))
            if isinstance(resolved, int) and resolved > 0:
                attrs["git_change_velocity"] = resolved

        velocity_items = [
            (path, commits)
            for path, commits in canonical_velocity.items()
            if isinstance(commits, int) and commits > 0
        ]
        if not velocity_items:
            for _node_id, attrs in module_graph.graph.nodes(data=True):
                commits = attrs.get("git_change_velocity")
                source_file = attrs.get("source_file")
                if (
                    isinstance(commits, int)
                    and commits > 0
                    and isinstance(source_file, str)
                    and source_file
                ):
                    velocity_items.append((source_file, commits))

        intelligence["high_velocity_files"] = [
            {"file": path, "commits": commits}
            for path, commits in sorted(velocity_items, key=lambda item: item[1], reverse=True)[:10]
        ]

        stage_order = {
            "extraction_config": 1,
            "loading_config": 2,
            "dbt_sources_schema": 3,
        }
        pipeline_items: list[dict[str, str]] = []
        for node_id, attrs in module_graph.graph.nodes(data=True):
            role = attrs.get("ingestion_role")
            if role not in stage_order:
                continue
            source_file = (
                attrs.get("source_file") if isinstance(attrs.get("source_file"), str) else ""
            )
            pipeline_items.append(
                {
                    "node": str(node_id),
                    "file": source_file or str(node_id),
                    "role": role,
                }
            )
        intelligence["ingestion_pipeline"] = sorted(
            pipeline_items,
            key=lambda item: (stage_order.get(item["role"], 99), item["file"], item["node"]),
        )

        macro_index: list[dict[str, str]] = []
        macro_folders: list[str] = []
        for _node_id, attrs in module_graph.graph.nodes(data=True):
            logical_name = attrs.get("logical_name")
            macro_args = attrs.get("macro_args")
            source_file = attrs.get("source_file")
            if not isinstance(logical_name, str) or not isinstance(macro_args, list):
                continue
            if not isinstance(source_file, str) or not source_file:
                continue
            normalized_source = normalize_path_key(source_file)
            macro_index.append({"name": logical_name, "source_file": normalized_source})
            parts = normalized_source.split("/")
            for marker in ("5_macros", "macros"):
                if marker in parts:
                    marker_index = parts.index(marker)
                    macro_folders.append("/".join(parts[: marker_index + 1]) + "/")
                    break

        if macro_index:
            macro_index = sorted(macro_index, key=lambda item: (item["name"], item["source_file"]))
            folder_counter = Counter(macro_folders)
            intelligence["macro_summary"] = {
                "macro_folder": folder_counter.most_common(1)[0][0] if folder_counter else "",
                "macro_count": len(macro_index),
                "key_macros": macro_index[:5],
            }
        else:
            intelligence["macro_summary"] = {
                "macro_folder": "",
                "macro_count": 0,
                "key_macros": [],
            }
        return intelligence

    # ------------------------------------------------------------------
    # Incremental mode helpers
    # ------------------------------------------------------------------
    def _state_path(self) -> str:
        return os.path.join(self.output_dir, "state.json")

    def _get_current_commit_hash(self) -> str | None:
        try:
            result = subprocess.run(
                ["git", "-C", self.target_dir, "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            )
            return result.stdout.strip()
        except Exception:
            return None

    def _load_previous_commit_hash(self) -> str | None:
        path = self._state_path()
        if not os.path.exists(path):
            return None
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            value = data.get("commit_hash")
            return value if isinstance(value, str) and value else None
        except Exception:
            return None

    def _persist_incremental_state(self, commit_hash: str | None) -> None:
        if not commit_hash:
            return
        path = self._state_path()
        os.makedirs(self.output_dir, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"commit_hash": commit_hash}, f, indent=2)
        logger.info("[Incremental] Saved state to %s", path)

    def _changed_files_between(self, old_hash: str, new_hash: str) -> set[str]:
        result = subprocess.run(
            ["git", "-C", self.target_dir, "diff", "--name-only", old_hash, new_hash],
            check=True,
            capture_output=True,
            text=True,
        )
        return {
            normalize_path_key(line.strip()) for line in result.stdout.splitlines() if line.strip()
        }

    def _load_graph_if_exists(self, filename: str, graph_name: str) -> KnowledgeGraph | None:
        path = os.path.join(self.output_dir, filename)
        if not os.path.exists(path):
            return None
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        graph = KnowledgeGraph(graph_name)
        graph.graph = nx.node_link_graph(data, edges="edges")
        return graph

    def _load_semantic_results_from_disk(self) -> dict:
        def _load(name: str, default):
            path = os.path.join(self.output_dir, name)
            if not os.path.exists(path):
                return default
            try:
                with open(path, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return default

        brief_path = os.path.join(self.output_dir, "onboarding_brief.md")
        day_one_answers = ""
        if os.path.exists(brief_path):
            try:
                with open(brief_path, encoding="utf-8") as f:
                    day_one_answers = f.read()
            except Exception:
                day_one_answers = ""
        return {
            "purpose_statements": _load("purpose_statements.json", {}),
            "drift_flags": _load("drift_flags.json", {}),
            "domain_map": _load("domain_map.json", {}),
            "budget_summary": _load("budget_summary.json", {}),
            "day_one_answers": day_one_answers,
        }

    @staticmethod
    def _path_set_with_aliases(paths: set[str]) -> set[str]:
        aliased = set()
        for path in paths:
            norm = normalize_path_key(path)
            aliased.add(norm)
            aliased.add(norm.replace("/", "\\"))
        return aliased

    def _remove_changed_file_artifacts(
        self, graph: KnowledgeGraph, changed_files: set[str], include_edges: bool = True
    ) -> None:
        aliases = self._path_set_with_aliases(changed_files)
        nodes_to_remove = []
        for node_id, attrs in graph.graph.nodes(data=True):
            source_file = attrs.get("source_file")
            node_file = normalize_path_key(node_id) if isinstance(node_id, str) else None
            if source_file in aliases or node_file in aliases:
                nodes_to_remove.append(node_id)
        if nodes_to_remove:
            graph.graph.remove_nodes_from(nodes_to_remove)

        if include_edges:
            edges_to_remove = []
            for src, tgt, data in graph.graph.edges(data=True):
                source_file = data.get("source_file")
                if source_file in aliases:
                    edges_to_remove.append((src, tgt))
            if edges_to_remove:
                graph.graph.remove_edges_from(edges_to_remove)

    def _run_incremental_if_possible(self) -> bool:
        current_hash = self._get_current_commit_hash()
        if not current_hash:
            logger.info("[Incremental] No git commit hash detected; falling back to full analysis.")
            return False

        previous_hash = self._load_previous_commit_hash()
        if not previous_hash:
            logger.info("[Incremental] No previous state found; running full analysis.")
            return False

        module_graph = self._load_graph_if_exists("module_graph.json", "Module Graph")
        lineage_graph = self._load_graph_if_exists("lineage_graph.json", "Lineage Graph")
        if module_graph is None or lineage_graph is None:
            logger.info("[Incremental] Missing prior graph artifacts; running full analysis.")
            return False

        try:
            changed_files = self._changed_files_between(previous_hash, current_hash)
        except Exception as e:
            logger.warning("[Incremental] Could not compute changed files: %s", e)
            return False

        self.module_graph = module_graph
        self.lineage_graph = lineage_graph

        if not changed_files:
            logger.info("[Incremental] No changed files detected between commits.")
            semantic_results = self._load_semantic_results_from_disk()
            self.archivist.archive(
                self.module_graph, self.lineage_graph, semantic_results, git_velocity={}
            )
            self._persist_incremental_state(current_hash)
            return True

        logger.info("[Incremental] Re-analyzing %d changed file(s).", len(changed_files))
        self._remove_changed_file_artifacts(self.module_graph, changed_files, include_edges=True)
        self._remove_changed_file_artifacts(self.lineage_graph, changed_files, include_edges=True)

        git_velocity: dict[str, int] = {}
        router = LanguageRouter(self.target_dir)

        yaml_changed = any(path.endswith((".yml", ".yaml")) for path in changed_files)
        config = {"model_paths": ["models"], "macro_paths": ["macros"]}
        if yaml_changed:
            yaml_results = analyze_all_yaml_files(self.target_dir)
            config["model_paths"] = yaml_results.get("model_paths", ["models"])
            config["macro_paths"] = yaml_results.get("macro_paths", ["macros"])
            if yaml_results.get("project"):
                self.module_graph.add_node(yaml_results["project"])
            for dataset in yaml_results.get("datasets", []):
                self.module_graph.add_node(dataset)
                self.lineage_graph.add_node(dataset)

        for changed in changed_files:
            abs_path = os.path.join(self.target_dir, changed)
            if not os.path.exists(abs_path):
                continue

            suffix = os.path.splitext(changed)[1].lower()
            if suffix == ".py":
                module, datasets, edges = router.analyze_file(abs_path)
                velocity = get_git_change_velocity(self.target_dir, changed)
                with_path_aliases(git_velocity, changed, velocity)
                if module:
                    self.module_graph.add_node(module)
                    self.module_graph.graph.nodes[module.id]["git_change_velocity"] = velocity
                for dataset in datasets:
                    self.module_graph.add_node(dataset)
                    self.lineage_graph.add_node(dataset)
                for edge in edges:
                    self.module_graph.add_edge(edge)
                    self.lineage_graph.add_edge(edge)

            if suffix == ".sql":
                for edge in analyze_sql_file(abs_path):
                    edge.source_file = normalize_path_key(changed)
                    self.lineage_graph.add_edge(edge)
                    if edge.target_dataset not in self.lineage_graph.graph:
                        self.lineage_graph.graph.add_node(
                            edge.target_dataset, id=edge.target_dataset
                        )
                    if edge.source_dataset not in self.lineage_graph.graph:
                        self.lineage_graph.graph.add_node(
                            edge.source_dataset, id=edge.source_dataset
                        )

        orphaned_nodes = self.find_orphaned_nodes(self.lineage_graph.graph)
        for node_id in orphaned_nodes:
            self.lineage_graph.graph.nodes[node_id]["orphaned"] = True

        semantic_results = self._load_semantic_results_from_disk()
        self.archivist.archive(
            self.module_graph, self.lineage_graph, semantic_results, git_velocity=git_velocity
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            self.module_graph.save_json(os.path.join(self.output_dir, "module_graph.json"))
            self.lineage_graph.save_json(os.path.join(self.output_dir, "lineage_graph.json"))

        self._persist_incremental_state(current_hash)
        logger.info("[Incremental] Analysis complete. Results saved to %s", self.output_dir)
        return True
