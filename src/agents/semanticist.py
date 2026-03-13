"""
src/agents/semanticist.py

Phase 3 — The Semanticist Agent
LLM-powered purpose extraction, doc drift detection, domain clustering,
and Day-One question synthesis.

Model tier strategy (all via Ollama local API):
  - nomic-embed-text  → embeddings for cluster_into_domains()
  - qwen3:1.7b        → bulk purpose statements + doc drift (fast, cheap)
  - deepseek-r1:8b    → Day-One synthesis + onboarding brief (slow, thorough)
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

import httpx

from ..graph.knowledge_graph import KnowledgeGraph
from ..logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OLLAMA_BASE = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")

BULK_MODEL = "qwen3:1.7b"  # many calls — purpose statements, drift
SYNTHESIS_MODEL = "deepseek-r1:8b"  # few calls  — Day-One answers, brief
EMBED_MODEL = "nomic-embed-text"  # embeddings — domain clustering

BULK_TIMEOUT = 60  # seconds
SYNTHESIS_TIMEOUT = 300  # deepseek-r1 reasons, give it time


# ---------------------------------------------------------------------------
# ContextWindowBudget — tracks calls and estimated token spend
# ---------------------------------------------------------------------------


class ContextWindowBudget:
    """Lightweight tracker so we know how much we've spent per model."""

    def __init__(self):
        self._calls: dict[str, int] = {}
        self._est_tokens: dict[str, int] = {}

    def record(self, model: str, prompt_len: int):
        self._calls[model] = self._calls.get(model, 0) + 1
        # rough estimate: 1 token ≈ 4 chars
        self._est_tokens[model] = self._est_tokens.get(model, 0) + (prompt_len // 4)

    def summary(self) -> dict[str, Any]:
        return {
            "calls_per_model": dict(self._calls),
            "estimated_tokens_per_model": dict(self._est_tokens),
        }

    def log(self):
        logger.info("[ContextWindowBudget] %s", json.dumps(self.summary(), indent=2))


# ---------------------------------------------------------------------------
# Low-level Ollama helpers
# ---------------------------------------------------------------------------


def _ollama_generate(model: str, prompt: str, timeout: int = BULK_TIMEOUT) -> str:
    """
    Call Ollama /api/generate (non-streaming).
    Returns the response text or raises on failure.
    """
    url = f"{OLLAMA_BASE}/api/generate"
    payload = {"model": model, "prompt": prompt, "stream": False}
    try:
        r = httpx.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except httpx.HTTPStatusError as e:
        raise RuntimeError(f"Ollama HTTP error {e.response.status_code}: {e.response.text}") from e
    except httpx.ConnectError as e:
        raise RuntimeError(
            f"Cannot reach Ollama at {OLLAMA_BASE}. Is `ollama serve` running?"
        ) from e


def _ollama_embed(text: str) -> list[float]:
    """
    Call Ollama /api/embeddings with nomic-embed-text.
    Returns a float vector.
    """
    url = f"{OLLAMA_BASE}/api/embeddings"
    payload = {"model": EMBED_MODEL, "prompt": text}
    try:
        r = httpx.post(url, json=payload, timeout=60)
        r.raise_for_status()
        return r.json().get("embedding", [])
    except Exception as e:
        raise RuntimeError(f"Embedding call failed: {e}") from e


# ---------------------------------------------------------------------------
# Purpose Statement generation  (qwen3:1.7b — bulk)
# ---------------------------------------------------------------------------

PURPOSE_PROMPT = """\
You are a senior data engineer reviewing a codebase module.
Read the code below and write a 2-3 sentence PURPOSE STATEMENT.
Focus on WHAT the module does for the business, not HOW it works internally.
Do NOT repeat the docstring. Base your answer entirely on the code.

File: {path}
---
{code}
---
PURPOSE STATEMENT (2-3 sentences, business focus):"""


def generate_purpose_statement(
    path: str,
    code: str,
    budget: ContextWindowBudget,
) -> str:
    """Generate a purpose statement for one module using the bulk model."""
    # Truncate very large files to keep within context
    code_snippet = code[:6000] if len(code) > 6000 else code
    prompt = PURPOSE_PROMPT.format(path=path, code=code_snippet)
    budget.record(BULK_MODEL, len(prompt))
    try:
        return _ollama_generate(BULK_MODEL, prompt, timeout=BULK_TIMEOUT)
    except Exception as e:
        return f"[purpose extraction failed: {e}]"


# ---------------------------------------------------------------------------
# Doc drift detection  (qwen3:1.7b — bulk)
# ---------------------------------------------------------------------------

DRIFT_PROMPT = """\
Compare the DOCSTRING and the IMPLEMENTATION below.
Reply with exactly one of:
  ALIGNED   — docstring accurately describes what the code does
  DRIFT     — docstring contradicts or significantly misrepresents the code
  MISSING   — no docstring present

Then on the next line, one sentence explaining your verdict.

File: {path}
DOCSTRING:
{docstring}

IMPLEMENTATION (first 3000 chars):
{code}

VERDICT:"""


def detect_doc_drift(
    path: str,
    docstring: str,
    code: str,
    budget: ContextWindowBudget,
) -> tuple[str, str]:
    """
    Returns (verdict, explanation) where verdict is ALIGNED | DRIFT | MISSING.
    """
    if not docstring or not docstring.strip():
        return "MISSING", "No docstring found."

    prompt = DRIFT_PROMPT.format(
        path=path,
        docstring=docstring[:1000],
        code=code[:3000],
    )
    budget.record(BULK_MODEL, len(prompt))
    try:
        raw = _ollama_generate(BULK_MODEL, prompt, timeout=BULK_TIMEOUT)
        lines = [line.strip() for line in raw.strip().splitlines() if line.strip()]
        verdict = lines[0].upper() if lines else "UNKNOWN"
        explanation = lines[1] if len(lines) > 1 else ""
        # Normalise — model might say "DRIFT: ..." on one line
        for keyword in ("ALIGNED", "DRIFT", "MISSING"):
            if verdict.startswith(keyword):
                verdict = keyword
                break
        return verdict, explanation
    except Exception as e:
        return "UNKNOWN", f"drift check failed: {e}"


# ---------------------------------------------------------------------------
# Domain clustering  (nomic-embed-text + k-means)
# ---------------------------------------------------------------------------


def cluster_into_domains(
    purpose_statements: dict[str, str],
    k: int = 6,
) -> dict[str, str]:
    """
    Embed all purpose statements with nomic-embed-text, run k-means,
    then label each cluster with a short domain name using the bulk model.

    Returns {module_id: domain_label}.
    """
    if not purpose_statements:
        return {}

    try:
        import numpy as np
        from sklearn.cluster import KMeans
    except ImportError:
        logger.warning(
            "sklearn/numpy not available — skipping domain clustering. Run: uv add scikit-learn numpy"
        )
        return {mid: "unclustered" for mid in purpose_statements}

    ids = list(purpose_statements.keys())
    texts = [purpose_statements[i] for i in ids]

    logger.info("Embedding %d purpose statements...", len(texts))
    embeddings = []
    for text in texts:
        try:
            vec = _ollama_embed(text)
            embeddings.append(vec)
        except Exception as e:
            logger.warning("Embedding failed: %s", e)
            embeddings.append([0.0] * 768)

    X = np.array(embeddings)
    # Auto-adjust k if we have fewer items than clusters
    k = min(k, len(ids))
    if k < 2:
        return {mid: "single_domain" for mid in ids}

    km = KMeans(n_clusters=k, random_state=42, n_init="auto")
    labels = km.fit_predict(X)

    # Label each cluster: take up to 3 representative statements and ask the model
    cluster_texts: dict[int, list[str]] = {}
    for idx, label in enumerate(labels):
        cluster_texts.setdefault(int(label), []).append(texts[idx])

    cluster_names: dict[int, str] = {}
    for cluster_id, samples in cluster_texts.items():
        sample_block = "\n".join(f"- {s}" for s in samples[:3])
        label_prompt = (
            f"Given these module purpose statements from the same codebase cluster:\n"
            f"{sample_block}\n\n"
            f"Give a single short domain label (1-3 words, lowercase, e.g. 'ingestion', "
            f"'transformation', 'serving', 'monitoring', 'configuration'):\n"
        )
        try:
            name = _ollama_generate(BULK_MODEL, label_prompt, timeout=BULK_TIMEOUT)
            # Clean up — take first line, strip punctuation
            name = name.strip().splitlines()[0].strip().lower().strip(".'\"")
            cluster_names[cluster_id] = name
        except Exception:
            cluster_names[cluster_id] = f"domain_{cluster_id}"

    return {ids[i]: cluster_names[int(labels[i])] for i in range(len(ids))}


# ---------------------------------------------------------------------------
# Day-One question synthesis  (deepseek-r1:8b — synthesis)
# ---------------------------------------------------------------------------

DAY_ONE_PROMPT = """\
You are an expert data engineer onboarding to a new codebase.
Using the structural analysis and data lineage information below, answer the
FIVE FDE Day-One Questions. For each answer, cite specific file paths and
line numbers where possible.

=== MODULE GRAPH SUMMARY ===
{module_summary}

=== DATA LINEAGE SUMMARY ===
{lineage_summary}

=== MODULE PURPOSE STATEMENTS ===
{purpose_block}

=== GIT VELOCITY (commit count per file, last 30 days) ===
{velocity_block}

=== FIVE FDE DAY-ONE QUESTIONS ===
1. What is the primary data ingestion path?
2. What are the 3-5 most critical output datasets/endpoints?
3. What is the blast radius if the most critical module changes its interface?
4. Where is the business logic concentrated vs distributed?
5. What has changed most frequently in the last 30 days? (use the git velocity data above)

Answer each question with a clear heading Q1:, Q2:, etc.
Back every claim with a file path citation [file:line] where possible.
For Q5, rank files by commit count from the velocity data provided.
"""


def answer_day_one_questions(
    graph: KnowledgeGraph,
    purpose_statements: dict[str, str],
    budget: ContextWindowBudget,
    git_velocity: dict[str, int] | None = None,
) -> str:
    """
    Uses deepseek-r1:8b to synthesise Day-One answers from the full graph context.
    Returns the raw answer string (saved to onboarding_brief.md by Archivist).
    """
    module_summary = _summarise_modules(graph)
    lineage_summary = _summarise_lineage(graph)
    purpose_block = "\n".join(
        f"[{mid}]: {stmt[:200]}" for mid, stmt in list(purpose_statements.items())[:40]
    )

    # Format git velocity — top 20 by commit count, real data for Q5
    if git_velocity:
        top_files = sorted(git_velocity.items(), key=lambda x: x[1], reverse=True)[:20]
        velocity_block = "\n".join(
            f"  {commits:3d} commits — {path}" for path, commits in top_files
        )
    else:
        velocity_block = "  (no git history available)"

    prompt = DAY_ONE_PROMPT.format(
        module_summary=module_summary[:3000],
        lineage_summary=lineage_summary[:3000],
        purpose_block=purpose_block[:4000],
        velocity_block=velocity_block,
    )
    budget.record(SYNTHESIS_MODEL, len(prompt))
    logger.info("Running Day-One synthesis with %s (this may take 30-90s)...", SYNTHESIS_MODEL)
    t0 = time.time()
    result = _ollama_generate(SYNTHESIS_MODEL, prompt, timeout=SYNTHESIS_TIMEOUT)
    logger.info("Synthesis completed in %.1fs", time.time() - t0)
    return result


def _summarise_modules(graph: KnowledgeGraph) -> str:
    """Compact text summary of module nodes for the synthesis prompt."""
    lines = []
    for node_id, data in list(graph.graph.nodes(data=True))[:60]:
        file_type = data.get("file_type", "?")
        lines.append(f"  {node_id} [{file_type}]")
    return "Modules:\n" + "\n".join(lines)


def _summarise_lineage(graph: KnowledgeGraph) -> str:
    """Compact text summary of lineage edges for the synthesis prompt."""
    lines = []
    for u, v, data in list(graph.graph.edges(data=True))[:80]:
        t = data.get("transformation_type", "->")
        lines.append(f"  {u} --[{t}]--> {v}")
    return "Lineage edges:\n" + "\n".join(lines)


# ---------------------------------------------------------------------------
# Main Semanticist agent class
# ---------------------------------------------------------------------------


class Semanticist:
    """
    Phase 3 agent. Reads ModuleNodes from the KnowledgeGraph, enriches them
    with purpose statements, flags doc drift, clusters into domains, and
    synthesises the Five FDE Day-One Answers.

    Call order:
        semanticist = Semanticist(repo_path)
        results = semanticist.analyse(graph)
        # results keys: purpose_statements, drift_flags, domain_map, day_one_answers, budget
    """

    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.budget = ContextWindowBudget()

    # ------------------------------------------------------------------
    def analyse(
        self,
        graph: KnowledgeGraph,
        module_graph: KnowledgeGraph | None = None,
        git_velocity: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        """
        Full Semanticist pipeline. Returns enriched data dict.
        Batches calls by model to avoid VRAM thrashing.

        git_velocity: {file_path: commit_count} from Surveyor.
                      Passed into the Day-One synthesis so Q5 is grounded
                      in real git history rather than filename heuristics.
        """
        logger.info("===== Phase 3 Starting =====")

        # Collect modules with readable source files
        modules = self._collect_readable_modules(graph, module_graph=module_graph)
        logger.info("Found %d readable modules", len(modules))

        # ---- BATCH 1: nomic-embed prereqs (purpose statements needed first) ----
        # ---- BATCH 2: qwen3:1.7b — purpose statements (bulk) ------------------
        logger.info("Generating purpose statements (%s)...", BULK_MODEL)
        purpose_statements: dict[str, str] = {}
        for mod_id, code, _ in modules:
            stmt = generate_purpose_statement(mod_id, code, self.budget)
            purpose_statements[mod_id] = stmt
            logger.info("  ✓ %s", mod_id[:60])

        # ---- BATCH 3: qwen3:1.7b — doc drift detection (bulk) -----------------
        logger.info("Detecting doc drift (%s)...", BULK_MODEL)
        drift_flags: dict[str, dict[str, str]] = {}
        for mod_id, code, docstring in modules:
            verdict, explanation = detect_doc_drift(mod_id, docstring, code, self.budget)
            if verdict in ("DRIFT", "MISSING"):
                drift_flags[mod_id] = {"verdict": verdict, "explanation": explanation}
                logger.info("  ⚠ %s: %s", verdict, mod_id[:60])

        # ---- BATCH 4: nomic-embed-text — domain clustering --------------------
        logger.info("Clustering into domains (%s)...", EMBED_MODEL)
        domain_map = cluster_into_domains(purpose_statements)

        # ---- BATCH 5: deepseek-r1:8b — Day-One synthesis (once) --------------
        logger.info("Synthesising Day-One answers (%s)...", SYNTHESIS_MODEL)
        day_one_answers = answer_day_one_questions(
            graph,
            purpose_statements,
            self.budget,
            git_velocity=git_velocity or {},
        )

        self.budget.log()
        logger.info("===== Phase 3 Complete =====")

        return {
            "purpose_statements": purpose_statements,
            "drift_flags": drift_flags,
            "domain_map": domain_map,
            "day_one_answers": day_one_answers,
            "budget_summary": self.budget.summary(),
        }

    # ------------------------------------------------------------------
    def _collect_readable_modules(
        self,
        graph: KnowledgeGraph,
        module_graph: KnowledgeGraph | None = None,
    ) -> list[tuple[str, str, str]]:
        """
        Returns [(module_id, code_text, docstring), ...] for every node
        whose source file can be read from disk.
        """
        results = []
        source_graph = module_graph if module_graph is not None else graph
        for node_id, data in source_graph.graph.nodes(data=True):
            # Skip dynamic reference pseudo-nodes emitted by tree-sitter analyzer
            # (e.g. "<dynamic>:execute:load\loaders.py:35") — not real files
            if node_id.startswith("<dynamic>") or node_id.startswith("SELECT"):
                continue

            source_file = data.get("source_file") or data.get("id", "")
            if not source_file:
                continue

            # Resolve absolute path
            candidate = Path(source_file)
            if not candidate.is_absolute():
                candidate = self.repo_path / candidate

            if not candidate.exists():
                continue

            suffix = candidate.suffix.lower()
            if suffix not in (".py", ".sql", ".yml", ".yaml", ".md"):
                continue

            try:
                code = candidate.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue

            docstring = _extract_docstring(code, suffix)
            results.append((node_id, code, docstring))

        return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_docstring(code: str, suffix: str) -> str:
    """
    Best-effort docstring extraction.
    For .py: grab the first triple-quoted string.
    For others: grab the first comment block.
    """
    if suffix == ".py":
        import ast

        try:
            tree = ast.parse(code)
            return ast.get_docstring(tree) or ""
        except Exception:
            pass
    # Fallback: first block of # comments
    lines = code.splitlines()
    comment_lines = []
    for line in lines[:30]:
        stripped = line.strip()
        if stripped.startswith("#") or stripped.startswith("--"):
            comment_lines.append(stripped.lstrip("#-").strip())
        elif comment_lines:
            break
    return " ".join(comment_lines)
