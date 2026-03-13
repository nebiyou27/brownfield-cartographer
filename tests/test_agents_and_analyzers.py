from src.agents.archivist import Archivist, _is_macro, _is_pseudo
from src.agents.hydrologist import Hydrologist
from src.agents.semanticist import (
    ContextWindowBudget,
    _extract_docstring,
    _summarise_lineage,
    _summarise_modules,
    answer_day_one_questions,
    detect_doc_drift,
    generate_purpose_statement,
)
from src.agents.surveyor import Surveyor
from src.analyzers import git_analyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import DatasetNode, ModuleNode, TransformationEdge


def test_git_analyzer_success_and_walk(monkeypatch, tmp_path):
    class Result:
        stdout = "7\n"

    monkeypatch.setattr(git_analyzer.subprocess, "run", lambda *args, **kwargs: Result())
    assert git_analyzer.get_git_change_velocity(str(tmp_path), "file.py") == 7

    monkeypatch.setattr(git_analyzer, "get_git_change_velocity", lambda repo, rel: len(rel))
    (tmp_path / ".git").mkdir()
    (tmp_path / "a.py").write_text("", encoding="utf-8")
    nested = tmp_path / "pkg"
    nested.mkdir()
    (nested / "b.py").write_text("", encoding="utf-8")

    velocities = git_analyzer.get_all_file_velocities(str(tmp_path))

    assert velocities["a.py"] == len("a.py")
    assert velocities["pkg\\b.py"] == len("pkg\\b.py")


def test_hydrologist_trace_lineage_adds_nodes_and_edges(monkeypatch, tmp_path):
    repo_models = tmp_path / "models"
    repo_models.mkdir()
    repo_macros = tmp_path / "macros"
    repo_macros.mkdir()
    edge = TransformationEdge(
        source_dataset="raw.orders",
        target_dataset="stg.orders",
        source_file="models/stg_orders.sql",
    )
    monkeypatch.setattr(
        "src.agents.hydrologist.analyze_all_sql_files", lambda path, macros_dirs: [edge]
    )

    graph = KnowledgeGraph("lineage")
    dataset = DatasetNode(
        id="raw.orders",
        source_file="models/schema.yml",
        source_line=None,
        dataset_type="source",
    )
    precomputed = TransformationEdge(
        source_dataset="python.extract",
        target_dataset="raw.orders",
        source_file="src/extract.py",
        transformation_type="imports",
    )

    Hydrologist(str(tmp_path)).trace_lineage(
        graph,
        datasets=[dataset],
        model_paths=["models"],
        macro_paths=["macros"],
        precomputed_edges=[precomputed],
    )

    assert "raw.orders" in graph.graph.nodes
    assert graph.graph.has_edge("python.extract", "raw.orders")
    assert graph.graph.has_edge("raw.orders", "stg.orders")
    assert "stg.orders" in graph.graph.nodes


def test_surveyor_combines_yaml_and_python_analysis(monkeypatch, tmp_path):
    project = ModuleNode(
        id="project",
        source_file="dbt_project.yml",
        source_line=None,
        file_type="yaml",
        logical_name="project",
    )
    dataset = DatasetNode(
        id="orders",
        source_file="models/schema.yml",
        source_line=None,
        dataset_type="model",
    )
    py_module = ModuleNode(
        id="src/app.py",
        source_file="src/app.py",
        source_line=None,
        file_type="python",
        logical_name="app",
    )
    py_dataset = DatasetNode(
        id="derived.orders",
        source_file="src/app.py",
        source_line=None,
        dataset_type="model",
    )
    py_edge = TransformationEdge(
        source_dataset="orders",
        target_dataset="derived.orders",
        source_file="src/app.py",
        transformation_type="imports",
    )

    monkeypatch.setattr(
        "src.agents.surveyor.analyze_all_yaml_files",
        lambda repo_path: {
            "project": project,
            "datasets": [dataset],
            "model_paths": ["models"],
            "seed_paths": ["seeds"],
            "macro_paths": ["macros"],
        },
    )
    monkeypatch.setattr("src.agents.surveyor.get_git_change_velocity", lambda repo, rel: 5)

    class StubRouter:
        def __init__(self, repo_path):
            self.repo_path = repo_path

        def analyze_directory(self, repo_path):
            return [py_module], [py_dataset], [py_edge]

    monkeypatch.setattr("src.agents.surveyor.LanguageRouter", StubRouter)

    graph = KnowledgeGraph("module")
    results = Surveyor(str(tmp_path)).survey(graph)

    assert results["git_velocity"]["models\\schema.yml"] == 5
    assert results["git_velocity"]["src\\app.py"] == 5
    assert graph.graph.nodes["orders"]["git_change_velocity"] == 5
    assert graph.graph.has_edge("orders", "derived.orders")
    assert results["python_edges"] == [py_edge]


def test_archivist_helpers_and_archive(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")

    module_graph.graph.add_node("src/app.py", file_type="python")
    module_graph.graph.add_node("<dynamic>:temp", file_type="python")
    lineage_graph.graph.add_edge("raw.orders", "stg.orders", source_file="models/stg_orders.sql")
    lineage_graph.graph.add_edge("stg.orders", "mart.orders", source_file="models/mart_orders.sql")
    lineage_graph.graph.add_edge("mart.orders", "raw.orders", source_file="models/cycle.sql")
    lineage_graph.graph.add_node("macros\\helper.sql")
    lineage_graph.graph.add_node("orphan.table")
    lineage_graph.graph.add_node("broken.sql", parsed=False)

    semantic_results = {
        "purpose_statements": {"src\\app.py": "Runs the pipeline"},
        "domain_map": {"src\\app.py": "core"},
        "drift_flags": {"src\\app.py": {"verdict": "DRIFT", "explanation": "docs outdated"}},
        "budget_summary": {
            "calls_per_model": {"qwen": 2},
            "estimated_tokens_per_model": {"qwen": 50},
        },
    }

    archivist.archive(
        module_graph,
        lineage_graph,
        semantic_results,
        git_velocity={"src\\app.py": 4},
    )

    codebase = (tmp_path / ".cartography" / "CODEBASE.md").read_text(encoding="utf-8")
    audit = (tmp_path / ".cartography" / "audit_trace.log").read_text(encoding="utf-8")

    assert _is_pseudo("<dynamic>:temp") is True
    assert _is_macro("macros\\helper.sql") is True
    assert "Circular Dependencies: 1" in codebase
    assert "Orphaned Nodes: 2" in codebase
    assert "src\\app.py" in audit
    assert "qwen: 2 calls, ~50 tokens" in audit


def test_semanticist_helpers_and_generation(monkeypatch, tmp_path):
    budget = ContextWindowBudget()
    budget.record("model-a", 40)
    budget.record("model-a", 20)
    assert budget.summary()["estimated_tokens_per_model"]["model-a"] == 15

    monkeypatch.setattr(
        "src.agents.semanticist._ollama_generate",
        lambda model, prompt, timeout=60: "Purpose output",
    )
    assert generate_purpose_statement("mod.py", "x" * 7000, budget) == "Purpose output"

    monkeypatch.setattr(
        "src.agents.semanticist._ollama_generate",
        lambda model, prompt, timeout=60: "DRIFT\nMismatch",
    )
    verdict, explanation = detect_doc_drift("mod.py", "Docs", "code", budget)
    assert (verdict, explanation) == ("DRIFT", "Mismatch")
    assert detect_doc_drift("mod.py", "", "code", budget)[0] == "MISSING"

    assert _extract_docstring('"""Doc."""\n\nx = 1\n', ".py") == "Doc."
    assert _extract_docstring("-- SQL comment\nSELECT 1\n", ".sql") == "SQL comment"

    graph = KnowledgeGraph("module")
    graph.graph.add_node("src/app.py", file_type="python")
    graph.graph.add_edge("raw.orders", "stg.orders", transformation_type="sql_select")
    assert "src/app.py [python]" in _summarise_modules(graph)
    assert "raw.orders --[sql_select]--> stg.orders" in _summarise_lineage(graph)

    monkeypatch.setattr(
        "src.agents.semanticist._ollama_generate",
        lambda model, prompt, timeout=300: "Q1: answer",
    )
    answers = answer_day_one_questions(graph, {"src/app.py": "Purpose"}, budget, {"src/app.py": 9})
    assert answers == "Q1: answer"
