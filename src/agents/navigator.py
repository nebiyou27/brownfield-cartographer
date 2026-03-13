"""
src/agents/navigator.py

Phase 4 — The Navigator Agent
A LangGraph-powered query interface over the Cartographer's knowledge graph.

Four tools:
    find_implementation(concept)          — semantic search over purpose statements
    trace_lineage(dataset, direction)     — BFS upstream/downstream in lineage graph
    blast_radius(module_path)             — DFS downstream impact from a node
    explain_module(path)                  — LLM generative explanation with file context

All answers cite evidence: source file, line range, and analysis method
(static analysis vs. LLM inference).
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional

import httpx
import networkx as nx
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import END, START, MessagesState, StateGraph

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OLLAMA_BASE     = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
REASON_MODEL    = "deepseek-r1:8b"   # explain_module — needs reasoning
FAST_MODEL      = "qwen3:1.7b"       # tool routing + lightweight responses
EMBED_MODEL     = "nomic-embed-text" # find_implementation semantic search

CARTOGRAPHY_DIR = Path(__file__).resolve().parent.parent.parent / ".cartography"

SYSTEM_PROMPT = """You are the Brownfield Cartographer Navigator — an expert codebase 
intelligence agent. You have four tools to answer questions about the codebase:

- find_implementation: semantic search to locate where a concept is implemented
- trace_lineage: trace data flow upstream or downstream from a dataset
- blast_radius: find everything that breaks if a module changes its interface  
- explain_module: generate a detailed explanation of what a module does

Always cite evidence: file paths, line numbers, and whether the answer comes from 
static analysis or LLM inference. Be concise and precise."""


# ---------------------------------------------------------------------------
# Helpers — load cartography artifacts
# ---------------------------------------------------------------------------

def _load_json(filename: str) -> Dict:
    path = CARTOGRAPHY_DIR / filename
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _load_lineage_graph() -> nx.DiGraph:
    """Reconstruct NetworkX DiGraph from lineage_graph.json."""
    data = _load_json("lineage_graph.json")
    G = nx.DiGraph()
    for node in data.get("nodes", []):
        nid = node.get("id", "")
        if nid:
            G.add_node(nid, **node)
    for edge in data.get("edges", []):
        src = edge.get("source", "")
        tgt = edge.get("target", "")
        if src and tgt:
            G.add_edge(src, tgt, **edge)
    return G


def _embed(text: str) -> List[float]:
    url = f"{OLLAMA_BASE}/api/embeddings"
    r = httpx.post(url, json={"model": EMBED_MODEL, "prompt": text}, timeout=60)
    r.raise_for_status()
    return r.json().get("embedding", [])


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    import math
    dot   = sum(x * y for x, y in zip(a, b))
    normA = math.sqrt(sum(x * x for x in a))
    normB = math.sqrt(sum(x * x for x in b))
    return dot / (normA * normB) if normA and normB else 0.0


def _ollama_generate(model: str, prompt: str, timeout: int = 180) -> str:
    url = f"{OLLAMA_BASE}/api/generate"
    r = httpx.post(url, json={"model": model, "prompt": prompt, "stream": False}, timeout=timeout)
    r.raise_for_status()
    return r.json().get("response", "").strip()


# ---------------------------------------------------------------------------
# Tool 1 — find_implementation
# ---------------------------------------------------------------------------

@tool
def find_implementation(concept: str) -> str:
    """
    Semantic search over module purpose statements to find where a concept
    is implemented in the codebase.

    Args:
        concept: Natural language description of what you're looking for.
                 e.g. "revenue calculation logic" or "shapefile ingestion"

    Returns:
        Ranked list of modules most likely to implement the concept,
        with purpose statements and evidence source.
    """
    purpose_statements: Dict[str, str] = _load_json("purpose_statements.json")
    if not purpose_statements:
        return "[find_implementation] No purpose statements found. Run full analysis first."

    try:
        query_vec = _embed(concept)
        scores = []
        for mod_id, statement in purpose_statements.items():
            mod_vec = _embed(statement)
            score = _cosine(query_vec, mod_vec)
            scores.append((mod_id, score, statement))

        scores.sort(key=lambda x: x[1], reverse=True)
        top = scores[:5]

        lines = [f"[find_implementation] Top matches for: '{concept}'\n"
                 f"Evidence source: semantic search (nomic-embed-text embeddings)\n"]
        for rank, (mod_id, score, stmt) in enumerate(top, 1):
            lines.append(f"{rank}. [{score:.3f}] {mod_id}")
            lines.append(f"   Purpose: {stmt[:200]}")
        return "\n".join(lines)

    except Exception as e:
        return f"[find_implementation] Error: {e}"


# ---------------------------------------------------------------------------
# Tool 2 — trace_lineage
# ---------------------------------------------------------------------------

@tool
def trace_lineage(dataset: str, direction: str = "upstream") -> str:
    """
    Trace the data lineage for a dataset — find all upstream sources or
    downstream consumers using BFS traversal of the lineage graph.

    Args:
        dataset:   The dataset/table name to trace (e.g. "ventes_immobilieres")
        direction: "upstream" to find sources, "downstream" to find consumers.
                   Defaults to "upstream".

    Returns:
        All nodes reachable in the given direction with edge metadata,
        including source files and transformation types.
    """
    G = _load_lineage_graph()

    if dataset not in G:
        # fuzzy match
        candidates = [n for n in G.nodes if dataset.lower() in n.lower()]
        if not candidates:
            return f"[trace_lineage] Dataset '{dataset}' not found in lineage graph."
        dataset = candidates[0]
        note = f"(fuzzy matched to '{dataset}')"
    else:
        note = ""

    if direction == "upstream":
        # BFS on reversed graph
        reachable = nx.bfs_tree(G.reverse(), dataset)
        edges_info = []
        for u, v in reachable.edges():
            # u is downstream, v is upstream in reversed BFS
            edge_data = G.get_edge_data(v, u) or {}
            tf_type   = edge_data.get("transformation_type", "->")
            src_file  = edge_data.get("source_file", "unknown")
            edges_info.append(f"  {v} --[{tf_type}]--> {u}  [file: {src_file}]")
        nodes = list(reachable.nodes())
        nodes.remove(dataset)
    else:
        reachable = nx.bfs_tree(G, dataset)
        edges_info = []
        for u, v in reachable.edges():
            edge_data = G.get_edge_data(u, v) or {}
            tf_type   = edge_data.get("transformation_type", "->")
            src_file  = edge_data.get("source_file", "unknown")
            edges_info.append(f"  {u} --[{tf_type}]--> {v}  [file: {src_file}]")
        nodes = list(reachable.nodes())
        nodes.remove(dataset)

    if not nodes:
        return (f"[trace_lineage] '{dataset}' {note} has no {direction} dependencies.\n"
                f"Evidence source: static analysis (lineage_graph.json)")

    result = [
        f"[trace_lineage] {direction.upper()} of '{dataset}' {note}",
        f"Evidence source: static analysis (lineage_graph.json)",
        f"Found {len(nodes)} node(s):\n",
    ]
    result.extend(edges_info[:40])  # cap output length
    if len(edges_info) > 40:
        result.append(f"  ... and {len(edges_info) - 40} more edges")
    return "\n".join(result)


# ---------------------------------------------------------------------------
# Tool 3 — blast_radius
# ---------------------------------------------------------------------------

@tool
def blast_radius(module_path: str) -> str:
    """
    Find the blast radius of a module — all downstream nodes that would be
    affected if this module changes its interface. Uses DFS traversal.

    Args:
        module_path: Path or name of the module/dataset.
                     e.g. "load/loaders.py" or "infos_communes"

    Returns:
        All downstream dependents with transformation edge metadata,
        ranked by distance from the source node.
    """
    G = _load_lineage_graph()

    # Try exact match, then normalised, then fuzzy
    node = None
    for candidate in [module_path, module_path.replace("/", "\\"), module_path.replace("\\", "/")]:
        if candidate in G:
            node = candidate
            break
    if node is None:
        candidates = [n for n in G.nodes if module_path.lower() in n.lower()]
        if not candidates:
            return f"[blast_radius] '{module_path}' not found in lineage graph."
        node = candidates[0]

    # DFS downstream
    descendants = nx.descendants(G, node)
    if not descendants:
        return (f"[blast_radius] '{node}' has no downstream dependents — safe to change.\n"
                f"Evidence source: static analysis (lineage_graph.json)")

    # BFS layers for distance
    layers = dict(nx.single_source_shortest_path_length(G, node))
    layers.pop(node, None)

    result = [
        f"[blast_radius] Impact of changing '{node}'",
        f"Evidence source: static analysis (lineage_graph.json)",
        f"Blast radius: {len(descendants)} downstream node(s)\n",
    ]

    by_distance: Dict[int, List[str]] = {}
    for n, dist in sorted(layers.items(), key=lambda x: x[1]):
        by_distance.setdefault(dist, []).append(n)

    for dist in sorted(by_distance.keys()):
        result.append(f"Distance {dist}:")
        for dep in by_distance[dist]:
            edge_data = {}
            for pred in G.predecessors(dep):
                if pred in descendants or pred == node:
                    edge_data = G.get_edge_data(pred, dep) or {}
                    break
            src_file = edge_data.get("source_file", "unknown")
            result.append(f"  - {dep}  [file: {src_file}]")

    return "\n".join(result)


# ---------------------------------------------------------------------------
# Tool 4 — explain_module
# ---------------------------------------------------------------------------

@tool
def explain_module(path: str) -> str:
    """
    Generate a detailed explanation of what a module does, grounded in its
    actual source code. Uses deepseek-r1:8b for reasoning.

    Args:
        path: File path relative to the repo root.
              e.g. "load/loaders.py" or "1_data/prepare/geographie/infos_communes.sql"

    Returns:
        A detailed explanation of the module's purpose, key functions,
        and role in the data pipeline, with evidence citations.
    """
    # Try to find the file relative to cartography dir's parent (repo root)
    repo_root = CARTOGRAPHY_DIR.parent
    candidates = [
        repo_root / path,
        repo_root / path.replace("/", "\\"),
        repo_root / path.replace("\\", "/"),
    ]
    # Also search subdirectories
    for pattern in [path, path.replace("\\", "/")]:
        found = list(repo_root.rglob(Path(pattern).name))
        candidates.extend(found)

    file_path = None
    for c in candidates:
        if c.exists():
            file_path = c
            break

    purpose_statements = _load_json("purpose_statements.json")
    existing_purpose = (
        purpose_statements.get(path)
        or purpose_statements.get(path.replace("/", "\\"))
        or purpose_statements.get(path.replace("\\", "/"))
    )

    if file_path and file_path.exists():
        try:
            code = file_path.read_text(encoding="utf-8", errors="replace")[:6000]
        except Exception:
            code = "(could not read file)"
    else:
        code = "(file not found on disk — explaining from graph context only)"

    # Get lineage context
    G = _load_lineage_graph()
    mod_name = Path(path).stem
    upstream   = [u for u, _ in G.in_edges(mod_name)][:5]
    downstream = [v for _, v in G.out_edges(mod_name)][:5]

    prompt = f"""You are a senior data engineer explaining a codebase module to a new team member.

File: {path}
Upstream dependencies: {upstream or 'none'}
Downstream dependents: {downstream or 'none'}
Existing purpose summary: {existing_purpose or 'none'}

Source code:
---
{code}
---

Provide a clear, detailed explanation covering:
1. What this module does (business purpose, not implementation detail)
2. Key functions or SQL transformations it performs
3. Its role in the data pipeline
4. Any notable patterns, risks, or debt you observe

Cite specific function names or line references where relevant.
Evidence source: static analysis + LLM inference (deepseek-r1:8b)"""

    try:
        explanation = _ollama_generate(REASON_MODEL, prompt, timeout=240)
        return f"[explain_module] {path}\nEvidence: static analysis + LLM inference\n\n{explanation}"
    except Exception as e:
        return f"[explain_module] Error generating explanation: {e}"


# ---------------------------------------------------------------------------
# LangGraph state + nodes
# ---------------------------------------------------------------------------

TOOLS = [find_implementation, trace_lineage, blast_radius, explain_module]

_TOOL_MAP = {t.name: t for t in TOOLS}

_SYNTHESIS_PROMPT = """You are a codebase intelligence assistant. 
A tool has been called and returned raw results. Synthesise a clear, helpful answer 
for the user. Preserve all file paths, node names, and evidence citations exactly.
Be concise. Do not invent information beyond what the tool returned.

User question: {question}

Tool result:
{tool_result}"""


def _route(question: str) -> tuple[str, dict]:
    """Route question to the right tool using keyword heuristics. No LLM needed."""
    q = question.lower()

    # blast_radius — must check before lineage
    if any(w in q for w in ["blast", "breaks", "break if", "impact of changing", "affect if", "what breaks"]):
        words = question.replace("?", "").split()
        module = next(
            (w for w in reversed(words) if "/" in w or "\\" in w or ".py" in w or ".sql" in w or "_" in w),
            words[-1]
        )
        return "blast_radius", {"module_path": module}

    # trace_lineage — upstream/downstream/sources/produces/how does X reach Y
    if any(w in q for w in ["upstream", "downstream", "feed", "feeds", "sources", "produces", "consumer",
                             "depends on", "what produces", "what feeds", "where does", "lineage",
                             "how does", "walk through", "transformation", "transformed", "reaches",
                             "get to", "flow", "pipeline"]):
        direction = "downstream" if any(w in q for w in ["downstream", "consumer", "depends on"]) else "upstream"
        # For "how does X reach Y" — prefer the destination node (last snake_case token)
        words = question.replace("?", "").split()
        dataset = next(
            (w.strip("'\"`,") for w in reversed(words) if "_" in w and len(w) > 3),
            words[-1]
        )
        return "trace_lineage", {"dataset": dataset, "direction": direction}

    # explain_module — explain/describe/what does/purpose
    if any(w in q for w in ["explain", "describe", "what does", "purpose of", "what is"]):
        words = question.replace("?", "").split()
        path = next(
            (w for w in reversed(words) if "/" in w or "\\" in w or ".py" in w or ".sql" in w or "_" in w),
            words[-1]
        )
        return "explain_module", {"path": path}

    # find_implementation — where/find/locate/which module/implement
    if any(w in q for w in ["where", "find", "locate", "which module", "implement", "who handles"]):
        return "find_implementation", {"concept": question}

    # Default: semantic search
    return "find_implementation", {"concept": question}


def _route_multi(question: str) -> list[tuple[str, dict]]:
    """
    For complex multi-hop questions, return a sequence of tool calls.
    Detects transformation chain queries and expands them.
    """
    q = question.lower()

    # "how does X get transformed ... Y" — chain: upstream(Y) + upstream(intermediate) + explain each
    if any(w in q for w in ["how does", "walk through", "every step", "transformation chain"]):
        words = question.replace("?", "").split()
        # Find all snake_case tokens — first is likely source, last is likely destination
        snake_tokens = [w.strip("'\"`,") for w in words if "_" in w and len(w) > 3]
        if len(snake_tokens) >= 2:
            destination = snake_tokens[-1]
            source = snake_tokens[0]
            return [
                ("trace_lineage", {"dataset": destination, "direction": "upstream"}),
                ("trace_lineage", {"dataset": source, "direction": "downstream"}),
                ("explain_module", {"path": destination}),
            ]
        elif snake_tokens:
            destination = snake_tokens[-1]
            return [
                ("trace_lineage", {"dataset": destination, "direction": "upstream"}),
                ("explain_module", {"path": destination}),
            ]

    # Single tool fallback
    tool_name, args = _route(question)
    return [(tool_name, args)]


def _call_tool(tool_name: str, args: dict) -> str:
    """Directly invoke a tool function by name."""
    if tool_name not in _TOOL_MAP:
        return f"[Navigator] No tool matched for this query."
    tool_fn = _TOOL_MAP[tool_name]
    try:
        return tool_fn.invoke(args)
    except Exception as e:
        return f"[Navigator] Tool '{tool_name}' error: {e}"


def _synthesise(question: str, tool_result: str) -> str:
    """Ask the LLM to turn raw tool output into a clean answer."""
    prompt = _SYNTHESIS_PROMPT.format(question=question, tool_result=tool_result)
    try:
        return _ollama_generate(FAST_MODEL, prompt, timeout=90)
    except Exception:
        # If synthesis fails just return the raw result — still useful
        return tool_result


# ---------------------------------------------------------------------------
# LangGraph state graph (route → tool → synthesise)
# ---------------------------------------------------------------------------

class NavState(MessagesState):
    tool_calls: list    # list of (tool_name, args) tuples to execute
    tool_results: list  # accumulated results
    tool_result: str    # final combined result for synthesis


def _node_route(state: NavState) -> dict:
    question = state["messages"][-1].content
    calls = _route_multi(question)
    return {"tool_calls": calls, "tool_results": []}


def _node_tool(state: NavState) -> dict:
    results = list(state.get("tool_results", []))
    calls = state.get("tool_calls", [])
    if calls:
        tool_name, args = calls[0]
        result = _call_tool(tool_name, args)
        results.append(result)
    remaining = calls[1:]
    combined = "\n\n---\n\n".join(results)
    return {
        "tool_calls":   remaining,
        "tool_results": results,
        "tool_result":  combined,
    }


def _node_synthesise(state: NavState) -> dict:
    question = state["messages"][-1].content
    answer = _synthesise(question, state["tool_result"])
    return {"messages": [AIMessage(content=answer)]}


def _should_run_tool(state: NavState) -> str:
    return "tool" if state.get("tool_calls") else "synthesise"


def _more_tools(state: NavState) -> str:
    """After a tool call, check if more tools remain."""
    return "tool" if state.get("tool_calls") else "synthesise"


def _build_graph():
    graph = StateGraph(NavState)
    graph.add_node("route",      _node_route)
    graph.add_node("tool",       _node_tool)
    graph.add_node("synthesise", _node_synthesise)

    graph.add_edge(START, "route")
    graph.add_conditional_edges("route", _should_run_tool, {
        "tool":       "tool",
        "synthesise": "synthesise",
    })
    graph.add_conditional_edges("tool", _more_tools, {
        "tool":       "tool",
        "synthesise": "synthesise",
    })
    graph.add_edge("synthesise", END)

    return graph.compile()


class Navigator:
    def __init__(self):
        self.graph = _build_graph()

    def query(self, question: str) -> str:
        result = self.graph.invoke({
            "messages":     [HumanMessage(content=question)],
            "tool_calls":   [],
            "tool_results": [],
            "tool_result":  "",
        })
        return result["messages"][-1].content

    def run_interactive(self):
        print("\n[Navigator] Brownfield Cartographer Query Interface")
        print("[Navigator] Type 'exit' to quit\n")
        while True:
            try:
                question = input("Query> ").strip()
            except (KeyboardInterrupt, EOFError):
                print("\n[Navigator] Goodbye.")
                break
            if not question:
                continue
            if question.lower() in ("exit", "quit"):
                print("[Navigator] Goodbye.")
                break
            print("\n[Navigator] Thinking...\n")
            try:
                print(self.query(question))
            except Exception as e:
                print(f"[Navigator] Error: {e}")
            print()