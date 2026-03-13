import json

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

    def analyse(self, lineage_graph, module_graph, git_velocity):
        self.calls.append(
            {
                "lineage_graph": lineage_graph,
                "module_graph": module_graph,
                "git_velocity": git_velocity,
            }
        )
        return self.result


class FailingSemanticist(StubSemanticist):
    def analyse(self, lineage_graph, module_graph, git_velocity):
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


def build_orchestrator(monkeypatch, tmp_path, semanticist_cls=StubSemanticist, skip=False):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("src.orchestrator.Surveyor", StubSurveyor)
    monkeypatch.setattr("src.orchestrator.Hydrologist", StubHydrologist)
    monkeypatch.setattr("src.orchestrator.Semanticist", semanticist_cls)
    monkeypatch.setattr("src.orchestrator.Archivist", StubArchivist)
    return Orchestrator(str(tmp_path), skip_semanticist=skip)


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
