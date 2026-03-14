import json
import types

from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import ModuleNode
from src.orchestrator import Orchestrator


class StubSurveyor:
    def __init__(self, target_dir):
        self.target_dir = target_dir
        self.calls = []

    def survey(self, module_graph):
        self.calls.append(module_graph)
        module_graph.graph.add_node("module.py", id="module.py", source_file="module.py")
        return {
            "datasets": ["raw.orders"],
            "git_velocity": {"module.py": 3},
        }


class StubHydrologist:
    def __init__(self, target_dir):
        self.target_dir = target_dir
        self.calls = []

    def trace_lineage(
        self,
        lineage_graph,
        datasets,
        model_paths,
        macro_paths,
        precomputed_edges,
    ):
        self.calls.append(
            {
                "datasets": datasets,
                "model_paths": model_paths,
                "macro_paths": macro_paths,
                "precomputed_edges": precomputed_edges,
            }
        )
        lineage_graph.graph.add_edge(
            "raw.orders",
            "stg.orders",
            source_file="models/stg_orders.sql",
        )
        lineage_graph.graph.add_node(
            "broken.sql",
            parsed=True,
        )
        lineage_graph.graph.add_node("lonely.table")


class StubSemanticist:
    def __init__(self, target_dir):
        self.target_dir = target_dir
        self.calls = []
        self.result = {
            "purpose_statements": {"module.py": "Handles order mapping."},
            "drift_flags": {"module.py": {"verdict": "DRIFT", "explanation": "Outdated docs"}},
            "domain_map": {"module.py": "orders"},
            "budget_summary": {"calls_per_model": {"qwen": 1}},
            "day_one_answers": "Q1: Start here.",
        }

    def analyse(self, lineage_graph, module_graph, git_velocity, graph_intelligence):
        self.calls.append(
            {
                "lineage_graph": lineage_graph,
                "module_graph": module_graph,
                "git_velocity": git_velocity,
                "graph_intelligence": graph_intelligence,
            }
        )
        return self.result


class FailingSemanticist(StubSemanticist):
    def analyse(self, lineage_graph, module_graph, git_velocity, graph_intelligence):
        raise RuntimeError("semantic failure")


class StubArchivist:
    def __init__(self, target_dir):
        self.target_dir = target_dir
        self.calls = []

    def archive(self, module_graph, lineage_graph, semantic_results, git_velocity):
        self.calls.append(
            {
                "module_graph": module_graph,
                "lineage_graph": lineage_graph,
                "semantic_results": semantic_results,
                "git_velocity": git_velocity,
            }
        )


def build_orchestrator(
    monkeypatch, tmp_path, semanticist_cls=StubSemanticist, skip=False, incremental=False
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("src.orchestrator.Surveyor", StubSurveyor)
    monkeypatch.setattr("src.orchestrator.Hydrologist", StubHydrologist)
    monkeypatch.setattr("src.orchestrator.Semanticist", semanticist_cls)
    monkeypatch.setattr("src.orchestrator.Archivist", StubArchivist)
    return Orchestrator(str(tmp_path), skip_semanticist=skip, incremental=incremental)


def test_run_analysis_saves_outputs_and_marks_orphans(monkeypatch, tmp_path):
    orchestrator = build_orchestrator(monkeypatch, tmp_path)

    orchestrator.run_analysis()

    assert orchestrator.hydrologist.calls == [
        {
            "datasets": ["raw.orders"],
            "model_paths": ["models"],
            "macro_paths": ["macros"],
            "precomputed_edges": [],
        }
    ]
    assert len(orchestrator.semanticist.calls) == 1
    intelligence = orchestrator.semanticist.calls[0]["graph_intelligence"]
    assert set(intelligence.keys()) == {
        "critical_nodes",
        "true_sources",
        "true_sinks",
        "blast_radius_top5",
        "high_velocity_files",
    }
    assert intelligence["critical_nodes"]
    assert intelligence["high_velocity_files"][0]["file"] == "module.py"
    assert len(orchestrator.archivist.calls) == 1
    assert orchestrator.lineage_graph.graph.nodes["broken.sql"]["orphaned"] is True
    assert orchestrator.lineage_graph.graph.nodes["lonely.table"]["orphaned"] is True

    output_dir = tmp_path / ".cartography"
    assert (output_dir / "module_graph.json").exists()
    assert (output_dir / "lineage_graph.json").exists()
    assert (output_dir / "purpose_statements.json").exists()
    assert (output_dir / "drift_flags.json").exists()
    assert (output_dir / "domain_map.json").exists()
    assert (output_dir / "budget_summary.json").exists()
    assert (
        (output_dir / "onboarding_brief.md").read_text(encoding="utf-8").endswith("Q1: Start here.")
    )
    assert "raw.orders -> stg.orders" in (tmp_path / "lineage_final.txt").read_text(
        encoding="utf-8"
    )


def test_run_analysis_skips_semanticist_when_flag_set(monkeypatch, tmp_path):
    orchestrator = build_orchestrator(monkeypatch, tmp_path, skip=True)

    orchestrator.run_analysis()

    assert orchestrator.semanticist.calls == []
    assert len(orchestrator.archivist.calls) == 1
    assert orchestrator.archivist.calls[0]["semantic_results"] == {}
    assert orchestrator.archivist.calls[0]["git_velocity"] == {"module.py": 3}
    assert not (tmp_path / ".cartography" / "purpose_statements.json").exists()


def test_run_analysis_continues_when_semanticist_fails(monkeypatch, tmp_path):
    orchestrator = build_orchestrator(monkeypatch, tmp_path, semanticist_cls=FailingSemanticist)

    orchestrator.run_analysis()

    assert len(orchestrator.archivist.calls) == 1
    assert orchestrator.archivist.calls[0]["semantic_results"] == {}
    assert orchestrator.archivist.calls[0]["git_velocity"] == {"module.py": 3}
    assert (tmp_path / ".cartography" / "lineage_graph.json").exists()
    assert (tmp_path / "lineage_final.txt").exists()


def test_save_semantic_results_writes_expected_files(monkeypatch, tmp_path):
    orchestrator = build_orchestrator(monkeypatch, tmp_path)
    results = {
        "purpose_statements": {"a.py": "Purpose"},
        "drift_flags": {"a.py": {"verdict": "MISSING", "explanation": "none"}},
        "domain_map": {"a.py": "core"},
        "budget_summary": {"calls_per_model": {"qwen": 2}},
        "day_one_answers": "Q1: answer",
    }

    orchestrator._save_semantic_results(results)

    output_dir = tmp_path / ".cartography"
    assert json.loads((output_dir / "purpose_statements.json").read_text(encoding="utf-8")) == {
        "a.py": "Purpose"
    }
    assert json.loads((output_dir / "drift_flags.json").read_text(encoding="utf-8")) == {
        "a.py": {"verdict": "MISSING", "explanation": "none"}
    }
    assert json.loads((output_dir / "domain_map.json").read_text(encoding="utf-8")) == {
        "a.py": "core"
    }
    assert json.loads((output_dir / "budget_summary.json").read_text(encoding="utf-8")) == {
        "calls_per_model": {"qwen": 2}
    }
    assert (output_dir / "onboarding_brief.md").read_text(encoding="utf-8").endswith("Q1: answer")


def test_find_orphaned_nodes_returns_only_isolated_nodes(monkeypatch, tmp_path):
    orchestrator = build_orchestrator(monkeypatch, tmp_path)
    graph = orchestrator.lineage_graph.graph
    graph.add_edge("a", "b")
    graph.add_node("c")
    graph.add_node("d")

    orphaned = orchestrator.find_orphaned_nodes(graph)

    assert set(orphaned) == {"c", "d"}


def test_build_graph_intelligence_includes_blast_radius_and_high_velocity(monkeypatch, tmp_path):
    orchestrator = build_orchestrator(monkeypatch, tmp_path)
    lineage = KnowledgeGraph("lineage")
    lineage.graph.add_edge("raw.orders", "stg.orders", confidence=0.95, confidence_reason="sql")
    lineage.graph.add_edge("stg.orders", "mart.orders", confidence=0.70, confidence_reason="jinja")
    module = KnowledgeGraph("module")
    module.graph.add_node(
        "models/stg_orders.sql", source_file="models/stg_orders.sql", git_change_velocity=7
    )
    module.graph.add_node(
        "models/mart_orders.sql", source_file="models/mart_orders.sql", git_change_velocity=5
    )

    intelligence = orchestrator._build_graph_intelligence(
        lineage,
        module,
        {"models/stg_orders.sql": 7, "README.md": 3},
    )

    assert intelligence["critical_nodes"]
    assert "raw.orders" in intelligence["true_sources"]
    assert "mart.orders" in intelligence["true_sinks"]
    top_node = intelligence["critical_nodes"][0]["node"]
    assert top_node in intelligence["blast_radius_top5"]
    assert intelligence["high_velocity_files"][0] == {"file": "models/stg_orders.sql", "commits": 7}


def test_incremental_no_changes_uses_saved_graph_and_updates_state(monkeypatch, tmp_path):
    output_dir = tmp_path / ".cartography"
    output_dir.mkdir(parents=True, exist_ok=True)

    module_graph = KnowledgeGraph("module")
    module_graph.graph.add_node("module.py", source_file="module.py")
    module_graph.save_json(str(output_dir / "module_graph.json"))

    lineage_graph = KnowledgeGraph("lineage")
    lineage_graph.graph.add_edge("raw.orders", "stg.orders", source_file="models/stg_orders.sql")
    lineage_graph.save_json(str(output_dir / "lineage_graph.json"))

    (output_dir / "state.json").write_text('{"commit_hash":"oldhash"}', encoding="utf-8")
    (output_dir / "purpose_statements.json").write_text("{}", encoding="utf-8")
    (output_dir / "drift_flags.json").write_text("{}", encoding="utf-8")
    (output_dir / "domain_map.json").write_text("{}", encoding="utf-8")
    (output_dir / "budget_summary.json").write_text("{}", encoding="utf-8")

    def _run(cmd, check, capture_output, text):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return types.SimpleNamespace(stdout="newhash\n")
        if "diff" in cmd:
            return types.SimpleNamespace(stdout="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr("src.orchestrator.subprocess.run", _run)
    orchestrator = build_orchestrator(monkeypatch, tmp_path, incremental=True)

    orchestrator.run_analysis()

    assert orchestrator.surveyor.calls == []
    assert len(orchestrator.archivist.calls) == 1
    saved_state = json.loads((output_dir / "state.json").read_text(encoding="utf-8"))
    assert saved_state["commit_hash"] == "newhash"


def test_incremental_changed_python_file_reanalyzes_only_changed(monkeypatch, tmp_path):
    output_dir = tmp_path / ".cartography"
    output_dir.mkdir(parents=True, exist_ok=True)

    module_graph = KnowledgeGraph("module")
    module_graph.graph.add_node("old.py", source_file="old.py")
    module_graph.save_json(str(output_dir / "module_graph.json"))
    lineage_graph = KnowledgeGraph("lineage")
    lineage_graph.save_json(str(output_dir / "lineage_graph.json"))

    (output_dir / "state.json").write_text('{"commit_hash":"oldhash"}', encoding="utf-8")
    (output_dir / "purpose_statements.json").write_text("{}", encoding="utf-8")
    (output_dir / "drift_flags.json").write_text("{}", encoding="utf-8")
    (output_dir / "domain_map.json").write_text("{}", encoding="utf-8")
    (output_dir / "budget_summary.json").write_text("{}", encoding="utf-8")

    (tmp_path / "changed.py").write_text("x = 1\n", encoding="utf-8")

    def _run(cmd, check, capture_output, text):
        if cmd[-2:] == ["rev-parse", "HEAD"]:
            return types.SimpleNamespace(stdout="newhash\n")
        if "diff" in cmd:
            return types.SimpleNamespace(stdout="changed.py\n")
        raise AssertionError(f"Unexpected command: {cmd}")

    class StubRouter:
        def __init__(self, repo_path):
            self.repo_path = repo_path

        def analyze_file(self, file_path):
            mod = ModuleNode(
                id="changed.py",
                source_file="changed.py",
                source_line=1,
                file_type="python",
                logical_name="changed",
            )
            return mod, [], []

    monkeypatch.setattr("src.orchestrator.subprocess.run", _run)
    monkeypatch.setattr("src.orchestrator.LanguageRouter", StubRouter)
    monkeypatch.setattr("src.orchestrator.get_git_change_velocity", lambda repo, path: 2)
    orchestrator = build_orchestrator(monkeypatch, tmp_path, incremental=True)

    orchestrator.run_analysis()

    assert orchestrator.surveyor.calls == []
    assert "changed.py" in orchestrator.module_graph.graph.nodes
    assert orchestrator.module_graph.graph.nodes["changed.py"]["git_change_velocity"] == 2
