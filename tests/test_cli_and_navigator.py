import json
import sys
import types

import networkx as nx
import pytest

from src import cli
from src.agents import navigator


def test_cli_analyze_invokes_orchestrator(monkeypatch, tmp_path):
    calls = {}

    class StubOrchestrator:
        def __init__(self, repo_path, skip_semanticist):
            calls["init"] = (repo_path, skip_semanticist)

        def run_analysis(self):
            calls["ran"] = True

    monkeypatch.setattr(cli, "Orchestrator", StubOrchestrator)
    monkeypatch.setattr(sys, "argv", ["prog", "analyze", str(tmp_path), "--no-semanticist"])

    cli.main()

    assert calls["init"] == (str(tmp_path.resolve()), True)
    assert calls["ran"] is True


def test_cli_query_single_shot_uses_navigator(monkeypatch, tmp_path, capsys):
    cartography = tmp_path / ".cartography"
    cartography.mkdir()
    (cartography / "lineage_graph.json").write_text('{"nodes": [], "edges": []}', encoding="utf-8")

    class StubNavigator:
        def query(self, question):
            return f"answer:{question}"

    stub_module = types.ModuleType("src.agents.navigator")
    stub_module.Navigator = StubNavigator
    monkeypatch.setitem(sys.modules, "src.agents.navigator", stub_module)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "argv", ["prog", "query", str(tmp_path), "--ask", "Where is orders?"])

    cli.main()

    output = capsys.readouterr().out
    assert "[Navigator] Query: Where is orders?" in output
    assert "answer:Where is orders?" in output


def test_cli_exits_when_repo_path_missing(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["prog", "analyze", "missing-path"])

    with pytest.raises(SystemExit) as excinfo:
        cli.main()

    assert excinfo.value.code == 1


def test_load_json_and_lineage_graph(monkeypatch, tmp_path):
    monkeypatch.setattr(navigator, "CARTOGRAPHY_DIR", tmp_path)
    (tmp_path / "lineage_graph.json").write_text(
        json.dumps(
            {
                "nodes": [{"id": "raw.orders"}, {"id": "stg.orders"}],
                "edges": [
                    {
                        "source": "raw.orders",
                        "target": "stg.orders",
                        "source_file": "models/stg_orders.sql",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    data = navigator._load_json("lineage_graph.json")
    graph = navigator._load_lineage_graph()

    assert data["nodes"][0]["id"] == "raw.orders"
    assert set(graph.nodes) == {"raw.orders", "stg.orders"}
    assert graph.get_edge_data("raw.orders", "stg.orders")["source_file"] == "models/stg_orders.sql"


def test_find_implementation_ranks_by_embedding(monkeypatch):
    monkeypatch.setattr(
        navigator,
        "_load_json",
        lambda filename: {
            "orders.py": "Handles order ingestion",
            "billing.py": "Processes invoices",
        },
    )
    vectors = {
        "orders": [1.0, 0.0],
        "Handles order ingestion": [0.9, 0.1],
        "Processes invoices": [0.0, 1.0],
    }
    monkeypatch.setattr(navigator, "_embed", lambda text: vectors[text])

    result = navigator.find_implementation.invoke({"concept": "orders"})

    assert "Top matches" in result
    assert "1. [0.994] orders.py" in result


def test_trace_lineage_handles_upstream_downstream_and_missing(monkeypatch):
    graph = nx.DiGraph()
    graph.add_edge(
        "raw.orders",
        "stg.orders",
        transformation_type="sql_select",
        source_file="models/stg_orders.sql",
        confidence=0.95,
        confidence_reason="direct select parse",
    )
    graph.add_edge(
        "stg.orders",
        "mart.orders",
        transformation_type="aggregate",
        source_file="models/mart_orders.sql",
        confidence=0.70,
        confidence_reason="jinja placeholders reduced certainty",
    )
    monkeypatch.setattr(navigator, "_load_lineage_graph", lambda: graph)

    upstream = navigator.trace_lineage.invoke({"dataset": "mart.orders", "direction": "upstream"})
    downstream = navigator.trace_lineage.invoke(
        {"dataset": "raw.orders", "direction": "downstream"}
    )
    missing = navigator.trace_lineage.invoke({"dataset": "unknown", "direction": "upstream"})

    assert "raw.orders --[sql_select]--> stg.orders" in upstream
    assert "confidence: 0.95" in upstream
    assert "stg.orders --[aggregate]--> mart.orders" in downstream
    assert "confidence: 0.70" in downstream
    assert "not found in lineage graph" in missing


def test_blast_radius_and_explain_module(monkeypatch, tmp_path):
    graph = nx.DiGraph()
    graph.add_edge(
        "loaders.py",
        "stg.orders",
        source_file="models/stg_orders.sql",
        confidence=0.95,
        confidence_reason="structural import edge",
    )
    graph.add_edge(
        "stg.orders",
        "mart.orders",
        source_file="models/mart_orders.sql",
        confidence=0.70,
        confidence_reason="jinja placeholders reduced certainty",
    )
    monkeypatch.setattr(navigator, "_load_lineage_graph", lambda: graph)

    purpose_data = {"module.py": "Purpose summary"}
    monkeypatch.setattr(navigator, "_load_json", lambda filename: purpose_data)
    monkeypatch.setattr(
        navigator, "_ollama_generate", lambda model, prompt, timeout=240: "Detailed explanation"
    )

    monkeypatch.setattr(navigator, "CARTOGRAPHY_DIR", tmp_path / ".cartography")
    navigator.CARTOGRAPHY_DIR.mkdir()
    repo_root = navigator.CARTOGRAPHY_DIR.parent
    module_file = repo_root / "module.py"
    module_file.write_text("def run():\n    return 1\n", encoding="utf-8")

    blast = navigator.blast_radius.invoke({"module_path": "loaders.py"})
    explanation = navigator.explain_module.invoke({"path": "module.py"})

    assert "Blast radius: 2 downstream node(s)" in blast
    assert "path confidence: 0.70 ⚠️" in blast
    assert "Detailed explanation" in explanation
    assert "[explain_module] module.py" in explanation


def test_routing_and_tool_nodes(monkeypatch):
    assert navigator._route("What breaks if loaders.py changes?")[0] == "blast_radius"
    assert navigator._route("What produces mart_orders?")[0] == "trace_lineage"
    assert navigator._route("Explain module orders.py")[0] == "explain_module"
    assert navigator._route("Where is revenue logic implemented?")[0] == "find_implementation"
    assert len(navigator._route_multi("How does raw_orders get to mart_orders?")) == 3

    monkeypatch.setattr(navigator, "_call_tool", lambda tool_name, args: f"{tool_name}:{args}")
    route_state = navigator._node_route(
        {"messages": [types.SimpleNamespace(content="Explain x.py")]}
    )
    tool_state = navigator._node_tool({"tool_calls": [("find_implementation", {"concept": "x"})]})
    synth_state = navigator._node_synthesise(
        {
            "messages": [types.SimpleNamespace(content="q")],
            "tool_result": "tool output",
        }
    )

    assert route_state["tool_calls"][0][0] == "explain_module"
    assert tool_state["tool_result"] == "find_implementation:{'concept': 'x'}"
    assert isinstance(synth_state["messages"][0], navigator.AIMessage)


def test_synthesise_and_navigator_wrapper(monkeypatch):
    monkeypatch.setattr(navigator, "_ollama_generate", lambda model, prompt, timeout=90: "summary")
    assert navigator._synthesise("q", "raw") == "summary"

    monkeypatch.setattr(
        navigator,
        "_ollama_generate",
        lambda model, prompt, timeout=90: (_ for _ in ()).throw(RuntimeError("fail")),
    )
    assert navigator._synthesise("q", "raw") == "raw"

    class StubGraph:
        def invoke(self, state):
            return {"messages": [types.SimpleNamespace(content="final answer")]}

    monkeypatch.setattr(navigator, "_build_graph", lambda: StubGraph())
    nav = navigator.Navigator()
    assert nav.query("question") == "final answer"
