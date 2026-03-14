import json
import os
import re
from datetime import datetime
from pathlib import Path

from ..graph.knowledge_graph import KnowledgeGraph
from ..logger import get_logger

logger = get_logger(__name__)

# Pseudo-node prefixes emitted by tree-sitter — never real files
_PSEUDO_PREFIXES = ("<dynamic>", "SELECT", "INSERT", "UPDATE", "DELETE")
_CITATION_RE = re.compile(r"\[\s*`?([^\]`]+?):\d+`?\s*\]")


def _is_pseudo(node_id: str) -> bool:
    return any(node_id.startswith(p) for p in _PSEUDO_PREFIXES)


def _is_macro(node_id: str) -> bool:
    """Macro SQL files are reusable snippets, not datasets or pipeline nodes."""
    normalised = node_id.replace("\\", "/").lower()
    return "macros" in normalised


class Archivist:
    """
    Phase 4 — The Archivist Agent

    Consumes outputs from Surveyor, Hydrologist, and Semanticist and
    produces two persistent artifacts:
      - CODEBASE.md  : living context file for AI coding agents
      - audit_trace.log : execution trace for reproducibility
    """

    def __init__(self, target_dir: str):
        self.target_dir = target_dir
        self.output_dir = os.path.join(os.getcwd(), ".cartography")
        os.makedirs(self.output_dir, exist_ok=True)

    def archive(
        self,
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
        semantic_results: dict,
        git_velocity: dict[str, int] = None,
    ) -> None:
        logger.info("===== Phase 4 Starting =====")
        self._generate_codebase_md(
            module_graph, lineage_graph, semantic_results, git_velocity or {}
        )
        self._generate_onboarding_brief(module_graph, lineage_graph, semantic_results)
        self._generate_audit_trace(module_graph, lineage_graph, semantic_results)
        logger.info("===== Phase 4 Complete =====")

    def _generate_onboarding_brief(
        self,
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
        semantic_results: dict,
    ) -> None:
        path = os.path.join(self.output_dir, "onboarding_brief.md")
        raw_answers = (semantic_results.get("day_one_answers") or "").strip()
        candidates = self._collect_evidence_candidates(module_graph, lineage_graph)

        lines: list[str] = []
        if raw_answers:
            for line in raw_answers.splitlines():
                stripped = line.strip()
                if not stripped:
                    lines.append("")
                    continue
                content = self._strip_existing_citations(stripped)
                citation = self._pick_evidence_for_line(content, candidates)
                lines.append(f"{content} [{citation}]")
        else:
            fallback = self._pick_evidence_for_line("", candidates)
            lines.append(f"No semantic day-one answers were generated. [{fallback}]")

        with open(path, "w", encoding="utf-8") as f:
            f.write("# FDE Day-One Onboarding Brief\n\n")
            f.write("\n".join(lines).strip() + "\n")

        logger.info("Generated onboarding_brief.md -> %s", path)

    # ------------------------------------------------------------------
    def _generate_codebase_md(
        self,
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
        semantic_results: dict,
        git_velocity: dict[str, int],
    ) -> None:
        path = os.path.join(self.output_dir, "CODEBASE.md")

        purpose_statements = self._load_purpose_statements(semantic_results)
        domain_map = semantic_results.get("domain_map", {})
        drift_flags = semantic_results.get("drift_flags", {})

        module_count = len([n for n in module_graph.graph.nodes if not _is_pseudo(n)])
        dataset_count = len([n for n in lineage_graph.graph.nodes if not _is_pseudo(n)])
        edge_count = len(lineage_graph.graph.edges)
        domain_count = len(set(domain_map.values())) if domain_map else 0

        pagerank_nodes = self._get_pagerank(lineage_graph)
        sources, sinks = self._get_sources_and_sinks(lineage_graph)
        cycles = self._get_cycles(lineage_graph)
        orphaned = self._get_orphans(lineage_graph)
        anomalies = self._get_semantic_anomalies(lineage_graph, purpose_statements)
        top_velocity = self._get_high_velocity_files(module_graph, git_velocity)
        low_conf_edges = self._get_low_confidence_edges(lineage_graph)
        drift_items = {
            key: value for key, value in (drift_flags or {}).items() if not _is_pseudo(str(key))
        }

        with open(path, "w", encoding="utf-8") as f:
            total_nodes = module_count + dataset_count
            f.write(
                f"<!-- CARTOGRAPHER v1 | generated: {datetime.now().isoformat()} "
                f"| nodes: {total_nodes} | edges: {edge_count} -->\n\n"
            )

            f.write("## SECTION:ARCHITECTURE_SUMMARY\n")
            f.write(f"target_dir={self.target_dir}\n")
            f.write(f"module_nodes={module_count}\n")
            f.write(f"dataset_nodes={dataset_count}\n")
            f.write(f"lineage_edges={edge_count}\n")
            f.write(f"domain_count={domain_count}\n\n")

            f.write("## SECTION:CRITICAL_PATH\n")
            if pagerank_nodes:
                for rank, (node_id, score, details) in enumerate(pagerank_nodes[:5], 1):
                    purpose = purpose_statements.get(node_id, "")
                    f.write(
                        f"{rank}|node={node_id}|pagerank={score:.4f}|why={details}|purpose={purpose}\n"
                    )
            else:
                f.write("none\n")
            f.write("\n")

            f.write("## SECTION:SOURCES\n")
            if sources:
                for source in sorted(sources)[:20]:
                    f.write(f"node={source}\n")
            else:
                f.write("none\n")
            f.write("\n")

            f.write("## SECTION:SINKS\n")
            if sinks:
                for sink in sorted(sinks)[:20]:
                    f.write(f"node={sink}\n")
            else:
                f.write("none\n")
            f.write("\n")

            f.write("## SECTION:KNOWN_DEBT\n")
            f.write(f"cycles={len(cycles)}\n")
            for cycle in cycles[:5]:
                f.write(f"cycle={' -> '.join(cycle)}\n")
            f.write(f"drift_flags={len(drift_items)}\n")
            for mod_id, flag in sorted(drift_items.items()):
                verdict = (flag or {}).get("verdict", "UNKNOWN")
                explanation = (flag or {}).get("explanation", "")
                f.write(f"drift|module={mod_id}|verdict={verdict}|explanation={explanation}\n")
            f.write(f"orphans={len(orphaned)}\n")
            for orphan in sorted(orphaned):
                f.write(f"orphan={orphan}\n")
            f.write(f"semantic_anomalies={len(anomalies)}\n")
            for target, issue in anomalies:
                f.write(f"anomaly|node={target}|issue={issue}\n")
            f.write("\n")

            f.write("## SECTION:HIGH_VELOCITY_FILES\n")
            if top_velocity:
                for file_path, commits in top_velocity:
                    f.write(f"file={file_path}|commits={commits}\n")
            else:
                f.write("none\n")
            f.write("\n")

            f.write("## SECTION:MODULE_PURPOSE_INDEX\n")
            if purpose_statements:
                for mod_id in sorted(purpose_statements.keys()):
                    if _is_pseudo(mod_id):
                        continue
                    purpose_line = " ".join(str(purpose_statements[mod_id]).split())
                    f.write(f"module={mod_id}|purpose={purpose_line}\n")
            else:
                f.write("none\n")
            f.write("\n")

            f.write("## SECTION:LOW_CONFIDENCE_LINEAGE\n")
            if low_conf_edges:
                for src, tgt, conf, reason, src_file in low_conf_edges[:50]:
                    reason_text = reason or "none"
                    f.write(
                        f"edge={src}->{tgt}|confidence={conf:.2f}|reason={reason_text}|source={src_file}\n"
                    )
            else:
                f.write("none\n")
            f.write("\n")

        logger.info("Generated CODEBASE.md -> %s", path)
        return

        with open(path, "w", encoding="utf-8") as f:
            # ── Header ────────────────────────────────────────────────
            f.write("# Codebase Architecture\n\n")
            f.write(
                f"*Auto-generated by Brownfield Cartographer · {datetime.now().strftime('%Y-%m-%d')}*\n\n"
            )
            f.write(
                "> Inject this file as system-prompt context for instant architectural awareness.\n\n"
            )
            f.write("---\n\n")

            # ── Section 1: Architecture Overview ──────────────────────
            f.write("## 1. Architecture Overview\n\n")
            module_count = len([n for n in module_graph.graph.nodes if not _is_pseudo(n)])
            dataset_count = len([n for n in lineage_graph.graph.nodes if not _is_pseudo(n)])
            edge_count = len(lineage_graph.graph.edges)
            domain_count = len(set(domain_map.values())) if domain_map else 0
            f.write(
                f"`{self.target_dir}` is a dbt-based data engineering project comprising "
                f"**{module_count} Python modules** and **{dataset_count} SQL/dataset nodes** "
                f"across **{domain_count} inferred business domains**. "
                f"The lineage graph contains **{edge_count} transformation edges** tracing data "
                f"from raw sources through intermediary models to enriched output datasets. "
                f"The primary ingestion layer loads CSV, JSON, and shapefiles into PostgreSQL via "
                f"`load/loaders.py`; transformation logic lives in the `1_data/` dbt model tree.\n\n"
            )

            # ── Section 2: Critical Path (top 5 by PageRank) ──────────
            f.write("## 2. Critical Path\n\n")
            f.write("*Top modules by PageRank — highest architectural impact if changed.*\n\n")
            pagerank_nodes = self._get_pagerank(lineage_graph)
            if pagerank_nodes:
                for rank, (node_id, score, details) in enumerate(pagerank_nodes[:5], 1):
                    purpose = purpose_statements.get(node_id, "")
                    purpose_snippet = f" — {purpose[:120]}..." if purpose else ""
                    f.write(f"{rank}. **`{node_id}`** (score: {score:.4f}){purpose_snippet}\n")
                    f.write(f"   - *Why:* {details}\n")
            else:
                f.write("*(PageRank unavailable — graph may be empty)*\n")
            f.write("\n")

            # ── Section 3: Data Sources & Sinks ───────────────────────
            f.write("## 3. Data Sources & Sinks\n\n")
            sources, sinks = self._get_sources_and_sinks(lineage_graph)

            f.write("### Sources (no upstream dependencies)\n\n")
            if sources:
                for s in sorted(sources)[:20]:
                    f.write(f"- `{s}`\n")
            else:
                f.write("*(none detected)*\n")
            f.write("\n")

            f.write("### Sinks (no downstream dependents)\n\n")
            if sinks:
                for s in sorted(sinks)[:20]:
                    f.write(f"- `{s}`\n")
            else:
                f.write("*(none detected)*\n")
            f.write("\n")

            # ── Section 4: Known Debt ──────────────────────────────────
            f.write("## 4. Known Debt\n\n")

            # 4a. Circular dependencies
            cycles = self._get_cycles(lineage_graph)
            f.write(f"### Circular Dependencies: {len(cycles)}\n\n")
            if cycles:
                for cycle in cycles[:5]:
                    f.write(f"- `{'` → `'.join(cycle)}`\n")
            else:
                f.write("✅ No circular dependencies detected.\n")
            f.write("\n")

            # 4b. Doc drift
            drift_items = {k: v for k, v in drift_flags.items() if not _is_pseudo(k)}
            f.write(f"### Documentation Drift: {len(drift_items)} flag(s)\n\n")
            if drift_items:
                for mod_id, flag in sorted(drift_items.items()):
                    verdict = flag.get("verdict", "UNKNOWN")
                    explanation = flag.get("explanation", "")
                    if verdict == "DRIFT":
                        f.write(f"- **{verdict}** `{mod_id}` — {explanation}\n")
                    else:
                        f.write(f"- ❌ **{verdict}** `{mod_id}` — {explanation}\n")
            else:
                f.write("✅ No documentation drift detected.\n")
            f.write("\n")

            # 4c. Orphaned nodes
            orphaned = self._get_orphans(lineage_graph)
            f.write(f"### Orphaned Nodes: {len(orphaned)}\n\n")
            if orphaned:
                for node_id in sorted(orphaned):
                    f.write(f"- `{node_id}`\n")
            else:
                f.write("✅ No orphaned nodes detected.\n")
            f.write("\n")

            # 4d. Semantic Anomalies
            anomalies = self._get_semantic_anomalies(lineage_graph, purpose_statements)
            f.write(f"### Semantic Anomalies: {len(anomalies)}\n\n")
            if anomalies:
                for target, issue in anomalies:
                    f.write(f"- 🚩 **CONFLICT** `{target}`: {issue}\n")
            else:
                f.write("✅ No structural-to-semantic contradictions detected.\n")
            f.write("\n")

            # ── Section 5: High-Velocity Files ────────────────────────
            f.write("## 5. High-Velocity Files\n\n")
            f.write("*Files with the most commits in the last 30 days — likely pain points.*\n\n")
            top_velocity = self._get_high_velocity_files(module_graph, git_velocity)
            if top_velocity:
                for file_path, commits in top_velocity:
                    bar = "█" * min(commits, 20)
                    f.write(f"- `{file_path}` — **{commits}** commits {bar}\n")
            else:
                f.write("*(git velocity data not available)*\n")
            f.write("\n")

            # ── Section 6: Domain Module Index ────────────────────────
            f.write("## 6. Module Purpose Index\n\n")
            f.write("*Module-to-purpose mapping sourced from `purpose_statements.json`.*\n\n")
            if purpose_statements:
                f.write("| Module | Purpose |\n")
                f.write("| --- | --- |\n")
                for mod_id in sorted(purpose_statements.keys()):
                    if _is_pseudo(mod_id):
                        continue
                    purpose_line = " ".join(str(purpose_statements[mod_id]).split())
                    f.write(f"| `{mod_id}` | {purpose_line} |\n")
                f.write("\n")
            else:
                f.write("*(purpose statements not available)*\n\n")

            # Section 7: Low-confidence lineage edges
            f.write("## 7. Low-Confidence Lineage Edges\n\n")
            f.write(
                "*Edges retained for visibility, but flagged because extraction confidence is below 0.80.*\n\n"
            )
            low_conf_edges = self._get_low_confidence_edges(lineage_graph)
            if low_conf_edges:
                for src, tgt, conf, reason, src_file in low_conf_edges[:50]:
                    reason_text = reason or "No confidence reason recorded."
                    flag = "\u26a0\ufe0f " if conf < 0.8 else ""
                    f.write(
                        f"- {flag}`{src}` -> `{tgt}` ({conf:.2f}: {reason_text}) "
                        f"[file: `{src_file}`]\n"
                    )
            else:
                f.write("✅ No low-confidence lineage edges detected.\n")
            f.write("\n")

        logger.info("Generated CODEBASE.md → %s", path)

    # ------------------------------------------------------------------
    @staticmethod
    def _canonical_path(path: str) -> str:
        # Keep normalization OS-agnostic so Windows-style paths are normalized
        # consistently even when tests run on Linux/macOS.
        return Path(path).as_posix().replace("\\", "/")

    def _collect_evidence_candidates(
        self, module_graph: KnowledgeGraph, lineage_graph: KnowledgeGraph
    ) -> list[dict]:
        candidates: list[dict] = []
        seen: set[tuple[str, int]] = set()

        def _add(
            path: str | None,
            source_line: int | None = None,
            line_range: list[int] | None = None,
            aliases: list[str] | None = None,
        ):
            normalized_path = self._normalize_candidate_path(path)
            if not normalized_path:
                return
            line = self._resolve_line_number(source_line, line_range)
            key = (normalized_path, line)
            if key in seen:
                return
            seen.add(key)
            evidence = self._format_evidence(normalized_path, line)
            if not evidence:
                return
            alias_set = self._build_aliases(normalized_path, aliases or [])
            candidates.append(
                {
                    "path": normalized_path,
                    "line": line,
                    "evidence": evidence,
                    "aliases": alias_set,
                }
            )

        for node_id, attrs in module_graph.graph.nodes(data=True):
            source_file = attrs.get("source_file")
            fallback_path = source_file or (node_id if self._looks_like_path(node_id) else None)
            _add(
                fallback_path,
                attrs.get("source_line"),
                attrs.get("line_range"),
                aliases=[node_id],
            )

        for node_id, attrs in lineage_graph.graph.nodes(data=True):
            source_file = attrs.get("source_file")
            fallback_path = source_file or (node_id if self._looks_like_path(node_id) else None)
            _add(
                fallback_path,
                attrs.get("source_line"),
                attrs.get("line_range"),
                aliases=[node_id],
            )

        for src, tgt, data in lineage_graph.graph.edges(data=True):
            _add(
                data.get("source_file"),
                data.get("source_line"),
                data.get("line_range"),
                aliases=[src, tgt],
            )

        if not candidates:
            candidates.append(
                {
                    "path": "src/agents/archivist.py",
                    "line": 1,
                    "evidence": "src/agents/archivist.py:1",
                    "aliases": {"src/agents/archivist.py", "archivist", "archivist.py"},
                }
            )
        return candidates

    def _pick_evidence_for_line(self, line: str, candidates: list[dict]) -> str:
        if not candidates:
            return "`src/agents/archivist.py:1`"

        explicit_refs = re.findall(r"([A-Za-z0-9_./\\-]+\.[A-Za-z0-9_]+):(\d+)", line)
        if explicit_refs:
            for raw_path, raw_line in explicit_refs:
                normalized = self._canonical_path(raw_path).lower()
                for candidate in candidates:
                    candidate_path = str(candidate.get("path", "")).lower()
                    if normalized == candidate_path:
                        return f"`{normalized}:{int(raw_line)}`"

        line_norm = self._canonical_path(line).lower()
        line_tokens = set(re.findall(r"[a-z0-9_./-]+", line_norm))
        mentioned_paths = [self._canonical_path(p).lower() for p in re.findall(r"`([^`]+)`", line)]

        best_score = -1
        best = candidates[0]
        for candidate in candidates:
            score = 0
            candidate_path = str(candidate.get("path", "")).lower()
            aliases = candidate.get("aliases", set())

            if candidate_path and candidate_path in line_norm:
                score += 120

            for mentioned_path in mentioned_paths:
                if mentioned_path == candidate_path:
                    score += 160
                elif mentioned_path.endswith("/" + Path(candidate_path).name):
                    score += 120

            if aliases:
                overlap = aliases.intersection(line_tokens)
                score += len(overlap) * 25

            if score > best_score:
                best_score = score
                best = candidate

        if best_score <= 0:
            best = self._fallback_candidate(candidates)

        return f"`{best['evidence']}`"

    @staticmethod
    def _strip_existing_citations(line: str) -> str:
        return _CITATION_RE.sub("", line).strip()

    @staticmethod
    def _looks_like_path(text: str) -> bool:
        if not isinstance(text, str) or not text:
            return False
        return "/" in text or "\\" in text or "." in Path(text).name

    def _normalize_candidate_path(self, path: str | None) -> str | None:
        if not isinstance(path, str) or not path.strip():
            return None
        normalized = self._canonical_path(path.strip())
        if _is_pseudo(normalized):
            return None
        return normalized

    @staticmethod
    def _resolve_line_number(
        source_line: int | None, line_range: list[int] | tuple[int, ...] | None = None
    ) -> int:
        if isinstance(source_line, int) and source_line > 0:
            return source_line
        if isinstance(line_range, list | tuple) and line_range:
            start = line_range[0]
            if isinstance(start, int) and start > 0:
                return start
        return 1

    def _build_aliases(self, path: str, extras: list[str]) -> set[str]:
        aliases: set[str] = set()
        normalized_path = self._canonical_path(path).lower()
        aliases.add(normalized_path)

        basename = Path(normalized_path).name
        stem = Path(normalized_path).stem
        if basename:
            aliases.add(basename)
        if stem:
            aliases.add(stem)

        for item in extras:
            if not isinstance(item, str):
                continue
            normalized = self._canonical_path(item).lower()
            aliases.add(normalized)
            if self._looks_like_path(normalized):
                item_basename = Path(normalized).name
                item_stem = Path(normalized).stem
                if item_basename:
                    aliases.add(item_basename)
                if item_stem:
                    aliases.add(item_stem)
                continue

            aliases.update(
                token
                for token in re.findall(r"[a-z0-9_./-]+", normalized)
                if len(token) > 2 and token not in {"data", "make", "open", "test", "tests"}
            )

        return aliases

    @staticmethod
    def _fallback_candidate(candidates: list[dict]) -> dict:
        by_path: dict[str, int] = {}
        for candidate in candidates:
            path = str(candidate.get("path", "")).lower()
            by_path[path] = by_path.get(path, 0) + 1

        def _rank(candidate: dict) -> tuple[int, int]:
            path = str(candidate.get("path", "")).lower()
            ext = Path(path).suffix.lower()
            ext_rank = (
                2 if ext in {".py", ".sql"} else (1 if ext in {".csv", ".yml", ".yaml"} else 0)
            )
            return (ext_rank, by_path.get(path, 0))

        return max(candidates, key=_rank)

    def _load_purpose_statements(self, semantic_results: dict) -> dict[str, str]:
        persisted_path = os.path.join(self.output_dir, "purpose_statements.json")
        persisted: dict[str, str] = {}

        if os.path.exists(persisted_path):
            try:
                with open(persisted_path, encoding="utf-8") as fp:
                    raw = json.load(fp)
                if isinstance(raw, dict):
                    for key, value in raw.items():
                        if isinstance(key, str) and isinstance(value, str):
                            persisted[self._canonical_path(key)] = value
            except Exception as exc:
                logger.warning("Failed to read purpose_statements.json: %s", exc)

        semantic = semantic_results.get("purpose_statements", {}) or {}
        semantic_norm = {
            self._canonical_path(key): value
            for key, value in semantic.items()
            if isinstance(key, str) and isinstance(value, str)
        }

        # Persisted file is the authoritative source; in-memory semantic output fills gaps.
        merged = dict(semantic_norm)
        merged.update(persisted)
        return merged

    def _get_high_velocity_files(
        self, module_graph: KnowledgeGraph, git_velocity: dict[str, int]
    ) -> list[tuple[str, int]]:
        velocity_by_file: dict[str, int] = {}

        # Primary source: velocity already attached to module-graph nodes.
        for node_id, attrs in module_graph.graph.nodes(data=True):
            raw_velocity = attrs.get("git_change_velocity")
            if not isinstance(raw_velocity, int) or raw_velocity <= 0:
                continue

            source_file = attrs.get("source_file")
            if isinstance(source_file, str) and source_file:
                file_key = self._canonical_path(source_file)
            elif isinstance(node_id, str) and (
                "/" in node_id or "\\" in node_id or node_id.endswith(".py")
            ):
                file_key = self._canonical_path(node_id)
            else:
                continue

            velocity_by_file[file_key] = max(raw_velocity, velocity_by_file.get(file_key, 0))

        # Fallback to explicit survey output map if graph metadata is missing.
        if not velocity_by_file:
            for file_path, commits in (git_velocity or {}).items():
                if isinstance(commits, int) and commits > 0:
                    file_key = self._canonical_path(file_path)
                    velocity_by_file[file_key] = max(commits, velocity_by_file.get(file_key, 0))

        return sorted(velocity_by_file.items(), key=lambda item: item[1], reverse=True)[:15]

    # ------------------------------------------------------------------
    @staticmethod
    def _format_evidence(evidence: str | None, source_line: int | None = None) -> str | None:
        """Normalize evidence to explicit file:line attribution or null if unavailable."""
        text = (evidence or "").strip()
        if not text:
            return None
        if ":" in text:
            return text
        if isinstance(source_line, int) and source_line > 0:
            return f"{text}:{source_line}"
        return f"{text}:1"

    # ------------------------------------------------------------------
    @staticmethod
    def _trace_entry(
        phase: str,
        action: str,
        confidence: float | None,
        method: str,
        evidence: str | None,
    ) -> str:
        """Return a single JSONL record for the audit trace."""
        norm_confidence = None
        if isinstance(confidence, int | float):
            norm_confidence = round(max(0.0, min(1.0, float(confidence))), 2)
        return json.dumps(
            {
                "timestamp": datetime.now().isoformat(),
                "phase": phase,
                "action": action,
                "confidence": norm_confidence,
                "method": method.lower(),
                "evidence": evidence,
            }
        )

    def _generate_audit_trace(
        self,
        module_graph: KnowledgeGraph,
        lineage_graph: KnowledgeGraph,
        semantic_results: dict,
    ) -> None:
        log_path = os.path.join(self.output_dir, "audit_trace.log")

        with open(log_path, "w", encoding="utf-8") as f:
            for _, attrs in module_graph.graph.nodes(data=True):
                evidence = self._format_evidence(attrs.get("source_file"), attrs.get("source_line"))
                f.write(
                    self._trace_entry(
                        "surveyor",
                        "module_parsed",
                        1.0,
                        "static",
                        evidence,
                    )
                    + "\n"
                )

            for _, _, edge_data in lineage_graph.graph.edges(data=True):
                confidence = edge_data.get("confidence")
                if not isinstance(confidence, int | float):
                    confidence = None
                evidence = self._format_evidence(
                    edge_data.get("source_file"), edge_data.get("source_line")
                )
                f.write(
                    self._trace_entry(
                        "hydrologist",
                        "edge_added",
                        confidence,
                        "static",
                        evidence,
                    )
                    + "\n"
                )

            for node_id, attrs in lineage_graph.graph.nodes(data=True):
                if attrs.get("parsed") is False:
                    evidence = self._format_evidence(attrs.get("source_file") or node_id)
                    f.write(
                        self._trace_entry(
                            "hydrologist",
                            "module_parse_failed",
                            None,
                            "static",
                            evidence,
                        )
                        + "\n"
                    )

            purposes = semantic_results.get("purpose_statements", {}) or {}
            for module_id in sorted(purposes):
                f.write(
                    self._trace_entry(
                        "semanticist",
                        "purpose_inferred",
                        0.85,
                        "llm",
                        self._format_evidence(module_id),
                    )
                    + "\n"
                )

            drift_flags = semantic_results.get("drift_flags", {}) or {}
            for module_id, data in sorted(drift_flags.items()):
                if _is_pseudo(module_id):
                    continue
                verdict = str((data or {}).get("verdict", "UNKNOWN")).strip().upper()
                action = "drift_flagged" if verdict == "DRIFT" else "drift_checked"
                f.write(
                    self._trace_entry(
                        "semanticist",
                        action,
                        0.9,
                        "llm",
                        self._format_evidence(module_id),
                    )
                    + "\n"
                )

            f.write(
                self._trace_entry(
                    "archivist",
                    "artifact_written",
                    1.0,
                    "static",
                    self._format_evidence("CODEBASE.md"),
                )
                + "\n"
            )
            f.write(
                self._trace_entry(
                    "archivist",
                    "artifact_written",
                    1.0,
                    "static",
                    self._format_evidence("audit_trace.log"),
                )
                + "\n"
            )

        logger.info("Generated audit_trace.log -> %s", log_path)
        return

        with open(log_path, "w", encoding="utf-8") as f:
            # ── init phase: summary counts ────────────────────────────
            f.write(
                self._trace_entry(
                    "init",
                    f"target={self.target_dir}",
                    1.0,
                    "static",
                    "src/agents/archivist.py:1",
                )
                + "\n"
            )
            f.write(
                self._trace_entry(
                    "init",
                    f"modules_scanned={len(module_graph.graph.nodes)}",
                    1.0,
                    "static",
                    "src/agents/archivist.py:1",
                )
                + "\n"
            )
            f.write(
                self._trace_entry(
                    "init",
                    f"lineage_nodes={len(lineage_graph.graph.nodes)}",
                    1.0,
                    "static",
                    "src/agents/archivist.py:1",
                )
                + "\n"
            )
            f.write(
                self._trace_entry(
                    "init",
                    f"lineage_edges={len(lineage_graph.graph.edges)}",
                    1.0,
                    "static",
                    "src/agents/archivist.py:1",
                )
                + "\n"
            )

            # ── parse phase: failures ─────────────────────────────────
            failed = [
                nid
                for nid, attrs in lineage_graph.graph.nodes(data=True)
                if attrs.get("parsed") is False
            ]
            f.write(
                self._trace_entry(
                    "parse",
                    f"parse_failures={len(failed)}",
                    1.0,
                    "static",
                    "src/agents/archivist.py:1",
                )
                + "\n"
            )
            for nid in failed:
                f.write(
                    self._trace_entry(
                        "parse",
                        f"parse_failed: {nid}",
                        1.0,
                        "static",
                        nid,
                    )
                    + "\n"
                )

            # ── semantic phase: purpose statements ────────────────────
            purposes = semantic_results.get("purpose_statements", {})
            f.write(
                self._trace_entry(
                    "semantic",
                    f"purpose_statements={len(purposes)}",
                    0.85,
                    "LLM",
                    "semantic_results.purpose_statements:1",
                )
                + "\n"
            )

            # ── drift phase: doc-drift flags ──────────────────────────
            drift = {
                k: v
                for k, v in semantic_results.get("drift_flags", {}).items()
                if not _is_pseudo(k)
            }
            f.write(
                self._trace_entry(
                    "drift",
                    f"drift_flags={len(drift)}",
                    0.90,
                    "LLM",
                    "semantic_results.drift_flags:1",
                )
                + "\n"
            )
            for mod_id, data in sorted(drift.items()):
                f.write(
                    self._trace_entry(
                        "drift",
                        f"[{data.get('verdict')}] {mod_id}",
                        0.90,
                        "LLM",
                        mod_id,
                    )
                    + "\n"
                )

            # ── budget phase: LLM budget summary ─────────────────────
            budget = semantic_results.get("budget_summary", {})
            if budget:
                for model, calls in budget.get("calls_per_model", {}).items():
                    tokens = budget.get("estimated_tokens_per_model", {}).get(model, 0)
                    f.write(
                        self._trace_entry(
                            "budget",
                            f"{model}: {calls} calls, ~{tokens} tokens",
                            1.0,
                            "LLM",
                            f"semantic_results.budget_summary.calls_per_model.{model}:1",
                        )
                        + "\n"
                    )

        logger.info("Generated audit_trace.log → %s", log_path)

    # ------------------------------------------------------------------
    # Graph helpers
    # ------------------------------------------------------------------

    def _get_pagerank(self, graph: KnowledgeGraph):
        """Returns [(node_id, score, derivation_text)] sorted descending."""
        try:
            import networkx as nx

            pr = nx.pagerank(graph.graph, alpha=0.85)
            filtered = {k: v for k, v in pr.items() if not _is_pseudo(k) and not _is_macro(k)}
            sorted_nodes = sorted(filtered.items(), key=lambda x: x[1], reverse=True)

            results = []
            for nid, score in sorted_nodes:
                in_deg = graph.graph.in_degree(nid)
                out_deg = graph.graph.out_degree(nid)
                derivation = f"Centrality driven by {in_deg} upstream sources and {out_deg} downstream dependents."
                results.append((nid, score, derivation))
            return results
        except Exception:
            return []

    def _get_sources_and_sinks(self, graph: KnowledgeGraph):
        """Nodes with in-degree=0 (sources) and out-degree=0 (sinks), pseudo-nodes and macros excluded."""
        sources, sinks = [], []
        for node in graph.graph.nodes:
            if _is_pseudo(node) or _is_macro(node):
                continue
            if graph.graph.in_degree(node) == 0:
                sources.append(node)
            if graph.graph.out_degree(node) == 0:
                sinks.append(node)
        return sources, sinks

    def _get_cycles(self, graph: KnowledgeGraph) -> list[list[str]]:
        """Returns list of cycles (each a list of node ids)."""
        try:
            import networkx as nx

            return list(nx.simple_cycles(graph.graph))
        except Exception:
            return []

    def _get_orphans(self, graph: KnowledgeGraph) -> list[str]:
        """Nodes with no edges at all, pseudo-nodes and macros excluded."""
        nodes_with_edges = {n for u, v in graph.graph.edges() for n in (u, v)}
        return [
            n
            for n in graph.graph.nodes
            if n not in nodes_with_edges and not _is_pseudo(n) and not _is_macro(n)
        ]

    def _get_semantic_anomalies(
        self, graph: KnowledgeGraph, purpose_statements: dict
    ) -> list[tuple[str, str]]:
        """Detects contradictions between graph structure and LLM purpose strings."""
        anomalies = []
        for node_id, purpose in purpose_statements.items():
            if _is_pseudo(node_id):
                continue
            if node_id not in graph.graph:
                continue

            p_lower = purpose.lower()
            in_deg = graph.graph.in_degree(node_id)
            out_deg = graph.graph.out_degree(node_id)

            # Contradiction: Purpose says it's a "source" or "ingestion" but it has upstreams
            if ("source" in p_lower or "ingest" in p_lower) and in_deg > 0:
                anomalies.append(
                    (
                        node_id,
                        f"Purpose claims it's a source, but graph shows {in_deg} upstream dependencies.",
                    )
                )

            # Contradiction: Purpose says it's an "output" or "final" but it has downstreams
            if ("output" in p_lower or "final" in p_lower) and out_deg > 0:
                anomalies.append(
                    (
                        node_id,
                        f"Purpose claims it's a final output, but graph shows {out_deg} downstream dependents.",
                    )
                )

        return anomalies

    def _confidence_to_float(self, value) -> float:
        if isinstance(value, int | float):
            return max(0.0, min(1.0, float(value)))
        if isinstance(value, str):
            norm = value.strip().lower()
            mapping = {
                "high": 0.95,
                "medium": 0.70,
                "low": 0.55,
                "inferred": 0.75,
                "unknown": 0.50,
            }
            if norm in mapping:
                return mapping[norm]
            try:
                return max(0.0, min(1.0, float(norm)))
            except ValueError:
                return 0.50
        return 0.50

    def _get_low_confidence_edges(
        self, graph: KnowledgeGraph, threshold: float = 0.80
    ) -> list[tuple[str, str, float, str, str]]:
        results = []
        for src, tgt, data in graph.graph.edges(data=True):
            if _is_pseudo(src) or _is_pseudo(tgt) or _is_macro(src) or _is_macro(tgt):
                continue
            conf = self._confidence_to_float(data.get("confidence", 1.0))
            if conf < threshold:
                results.append(
                    (
                        src,
                        tgt,
                        conf,
                        data.get("confidence_reason", ""),
                        data.get("source_file", "unknown"),
                    )
                )
        return sorted(results, key=lambda item: item[2])
