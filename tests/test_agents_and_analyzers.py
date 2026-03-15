from src.agents.archivist import Archivist, _is_macro, _is_pseudo
from src.agents.hydrologist import Hydrologist
from src.agents.semanticist import (
    ContextWindowBudget,
    Semanticist,
    _extract_docstring,
    _summarise_lineage,
    _summarise_modules,
    answer_day_one_questions,
    detect_doc_drift,
    generate_purpose_statement,
    ollama_is_reachable,
)
from src.agents.surveyor import Surveyor
from src.analyzers import git_analyzer
from src.graph.knowledge_graph import KnowledgeGraph
from src.models.schemas import DatasetNode, MacroNode, ModuleNode, TransformationEdge


def test_git_analyzer_success_and_walk(monkeypatch, tmp_path):
    seen_commands = []

    class Result:
        def __init__(self, stdout):
            self.stdout = stdout

    def _run(*args, **kwargs):
        seen_commands.append(args[0])
        command = args[0]
        if "rev-parse" in command:
            return Result(str(tmp_path) + "\n")
        return Result("c1\nc2\nc3\n")

    monkeypatch.setattr(git_analyzer.subprocess, "run", _run)
    assert git_analyzer.get_git_change_velocity(str(tmp_path), "file.py") == 3
    assert seen_commands[1][:6] == [
        "git",
        "-C",
        str(tmp_path),
        "log",
        "--since=90.days",
        "--pretty=format:%H",
    ]

    monkeypatch.setattr(git_analyzer, "get_git_change_velocity", lambda repo, rel: len(rel))
    (tmp_path / ".git").mkdir()
    (tmp_path / "a.py").write_text("", encoding="utf-8")
    nested = tmp_path / "pkg"
    nested.mkdir()
    (nested / "b.py").write_text("", encoding="utf-8")

    velocities = git_analyzer.get_all_file_velocities(str(tmp_path))

    assert velocities["a.py"] == len("a.py")
    assert velocities["pkg\\b.py"] == len("pkg\\b.py")


def test_git_analyzer_honors_custom_days_window(monkeypatch, tmp_path):
    seen_commands = []

    class Result:
        def __init__(self, stdout):
            self.stdout = stdout

    def _run(*args, **kwargs):
        seen_commands.append(args[0])
        command = args[0]
        if "rev-parse" in command:
            return Result(str(tmp_path) + "\n")
        return Result("")

    monkeypatch.setattr(git_analyzer.subprocess, "run", _run)
    assert git_analyzer.get_git_change_velocity(str(tmp_path), "README.md", days=14) == 0
    assert "--since=14.days" in seen_commands[1]


def test_hydrologist_trace_lineage_adds_nodes_and_edges(monkeypatch, tmp_path):
    repo_models = tmp_path / "models"
    repo_models.mkdir()
    repo_macros = tmp_path / "macros"
    repo_macros.mkdir()
    edge = TransformationEdge(
        source_dataset="raw.orders",
        target_dataset="stg.orders",
        source_file="models/stg_orders.sql",
        confidence=0.95,
        confidence_reason="test edge",
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
        transformation_type="config",
        confidence=0.95,
        confidence_reason="precomputed",
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
    assert set(graph.graph.nodes["raw.orders"]["sources"]) == {"config", "python", "sql"}
    assert set(graph.graph.nodes["stg.orders"]["sources"]) == {"sql"}


def test_hydrologist_merges_duplicate_edges_and_preserves_edge_metadata(monkeypatch, tmp_path):
    repo_models = tmp_path / "models"
    repo_models.mkdir()
    sql_edge = TransformationEdge(
        source_dataset="raw.orders",
        target_dataset="stg.orders",
        source_file="models/stg_orders.sql",
        transformation_type="select",
        line_range=[1, 2],
        confidence=0.95,
        confidence_reason="sql parse",
    )
    python_edge = TransformationEdge(
        source_dataset="raw.orders",
        target_dataset="stg.orders",
        source_file="src/pipeline.py",
        transformation_type="python_read",
        line_range=[10, 10],
        confidence=0.70,
        confidence_reason="python read",
    )
    monkeypatch.setattr(
        "src.agents.hydrologist.analyze_all_sql_files", lambda path, macros_dirs: [sql_edge]
    )

    graph = KnowledgeGraph("lineage")
    Hydrologist(str(tmp_path)).trace_lineage(
        graph,
        datasets=[],
        model_paths=["models"],
        macro_paths=[],
        precomputed_edges=[python_edge],
    )

    edge_data = graph.graph.get_edge_data("raw.orders", "stg.orders")
    assert edge_data is not None
    assert set(edge_data["sources"]) == {"python", "sql"}
    assert len(edge_data.get("edge_variants", [])) >= 2


def test_hydrologist_includes_dag_config_edges(monkeypatch, tmp_path):
    repo_models = tmp_path / "models"
    repo_models.mkdir()

    monkeypatch.setattr(
        "src.agents.hydrologist.analyze_all_sql_files", lambda path, macros_dirs: []
    )
    config_edge = TransformationEdge(
        source_dataset="raw",
        target_dataset="orders",
        source_file="models/schema.yml",
        transformation_type="config",
        line_range=[1, 10],
        confidence=1.0,
        confidence_reason="explicit declaration",
        method="static",
    )
    monkeypatch.setattr(
        "src.agents.hydrologist.analyze_all_dag_config_files",
        lambda repo_path, model_paths: [config_edge],
    )

    graph = KnowledgeGraph("lineage")
    Hydrologist(str(tmp_path)).trace_lineage(
        graph,
        datasets=[],
        model_paths=["models"],
        macro_paths=[],
        precomputed_edges=[],
    )

    assert graph.graph.has_edge("raw", "orders")
    edge_data = graph.graph.get_edge_data("raw", "orders")
    assert edge_data["transformation_type"] == "config"
    assert edge_data["method"] == "static"


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
        transformation_type="config",
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
    monkeypatch.setattr(
        "src.agents.surveyor.get_all_file_velocities",
        lambda repo: {
            "models/schema.yml": 5,
            "models\\schema.yml": 5,
            "models/orders.sql": 8,
            "models\\orders.sql": 8,
            "README.md": 4,
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
    assert results["git_velocity"]["models\\orders.sql"] == 8
    assert results["git_velocity"]["README.md"] == 4
    assert graph.graph.nodes["orders"]["git_change_velocity"] == 5
    assert graph.graph.has_edge("orders", "derived.orders")
    assert results["python_edges"] == [py_edge]


def test_surveyor_registers_macro_nodes_from_macro_paths(monkeypatch, tmp_path):
    macro_dir = tmp_path / "macros"
    macro_dir.mkdir(parents=True, exist_ok=True)
    macro_sql = macro_dir / "helpers.sql"
    macro_sql.write_text(
        "{% macro normalize_city(name, country='KE') %}\nSELECT 1\n{% endmacro %}\n"
        "{% macro slugify(value) %}\nSELECT 2\n{% endmacro %}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "src.agents.surveyor.analyze_all_yaml_files",
        lambda repo_path: {
            "project": None,
            "datasets": [],
            "model_paths": ["models"],
            "seed_paths": ["seeds"],
            "macro_paths": ["macros"],
        },
    )
    monkeypatch.setattr(
        "src.agents.surveyor.get_all_file_velocities",
        lambda repo: {"macros/helpers.sql": 9, "macros\\helpers.sql": 9},
    )
    monkeypatch.setattr("src.agents.surveyor.get_git_change_velocity", lambda repo, rel: 9)

    class StubRouter:
        def __init__(self, repo_path):
            self.repo_path = repo_path

        def analyze_directory(self, repo_path):
            return [], [], []

    monkeypatch.setattr("src.agents.surveyor.LanguageRouter", StubRouter)

    graph = KnowledgeGraph("module")
    Surveyor(str(tmp_path)).survey(graph)

    macro_nodes = [
        (node_id, attrs)
        for node_id, attrs in graph.graph.nodes(data=True)
        if str(node_id).startswith("macro:")
    ]
    assert len(macro_nodes) == 2
    names = {attrs["logical_name"] for _, attrs in macro_nodes}
    assert names == {"normalize_city", "slugify"}
    normalize_node = next(
        attrs for _, attrs in macro_nodes if attrs["logical_name"] == "normalize_city"
    )
    assert normalize_node["macro_args"] == ["name", "country"]
    assert normalize_node["git_change_velocity"] == 9


def test_surveyor_tags_ingestion_config_and_dbt_source_schema(monkeypatch, tmp_path):
    extract_dir = tmp_path / "extract"
    load_dir = tmp_path / "load"
    source_dir = tmp_path / "1_data" / "sources"
    extract_dir.mkdir(parents=True, exist_ok=True)
    load_dir.mkdir(parents=True, exist_ok=True)
    source_dir.mkdir(parents=True, exist_ok=True)
    (extract_dir / "source_to_storage.yml").write_text("foo: bar\n", encoding="utf-8")
    (load_dir / "storage_to_pg.yml").write_text("foo: bar\n", encoding="utf-8")

    source_dataset = DatasetNode(
        id="cog_communes",
        source_file="1_data/sources/schema.yml",
        source_line=None,
        dataset_type="source",
    )

    monkeypatch.setattr(
        "src.agents.surveyor.analyze_all_yaml_files",
        lambda repo_path: {
            "project": None,
            "datasets": [source_dataset],
            "model_paths": ["models"],
            "seed_paths": ["seeds"],
            "macro_paths": ["macros"],
        },
    )
    monkeypatch.setattr(
        "src.agents.surveyor.get_all_file_velocities",
        lambda repo: {
            "extract/source_to_storage.yml": 3,
            "load/storage_to_pg.yml": 4,
            "1_data/sources/schema.yml": 2,
        },
    )
    monkeypatch.setattr("src.agents.surveyor.get_git_change_velocity", lambda repo, rel: 1)

    class StubRouter:
        def __init__(self, repo_path):
            self.repo_path = repo_path

        def analyze_directory(self, repo_path):
            return [], [], []

    monkeypatch.setattr("src.agents.surveyor.LanguageRouter", StubRouter)

    graph = KnowledgeGraph("module")
    Surveyor(str(tmp_path)).survey(graph)

    assert (
        graph.graph.nodes["extract/source_to_storage.yml"]["ingestion_role"] == "extraction_config"
    )
    assert graph.graph.nodes["load/storage_to_pg.yml"]["ingestion_role"] == "loading_config"
    assert graph.graph.nodes["1_data/sources/schema.yml"]["ingestion_role"] == "dbt_sources_schema"
    assert graph.graph.nodes["load/storage_to_pg.yml"]["git_change_velocity"] == 4


def test_archivist_helpers_and_archive(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")

    module_graph.graph.add_node("src/app.py", file_type="python")
    module_graph.graph.nodes["src/app.py"]["git_change_velocity"] = 6
    macro_node = MacroNode(
        id="macro:normalize_city:macros/helpers.sql:1",
        source_file="macros/helpers.sql",
        source_line=1,
        logical_name="normalize_city",
        macro_args=["name", "country"],
    )
    module_graph.add_node(macro_node)
    module_graph.graph.add_node("<dynamic>:temp", file_type="python")
    lineage_graph.graph.add_edge("raw.orders", "stg.orders", source_file="models/stg_orders.sql")
    lineage_graph.graph.add_edge("stg.orders", "mart.orders", source_file="models/mart_orders.sql")
    lineage_graph.graph.add_edge("mart.orders", "raw.orders", source_file="models/cycle.sql")
    lineage_graph.graph.add_edge(
        "stg.orders",
        "qa.orders_check",
        source_file="models/qa_orders_check.sql",
        confidence=0.70,
        confidence_reason="jinja placeholders reduced certainty",
    )
    lineage_graph.graph.add_node("macros\\helper.sql")
    lineage_graph.graph.add_node("orphan.table")
    lineage_graph.graph.add_node("broken.sql", parsed=False)

    semantic_results = {
        "purpose_statements": {
            "src\\app.py": "Runs the pipeline",
            "macro:normalize_city:macros/helpers.sql:1": "Normalizes city names for downstream joins.",
        },
        "domain_map": {"src\\app.py": "core"},
        "drift_flags": {"src\\app.py": {"verdict": "DRIFT", "explanation": "docs outdated"}},
        "day_one_answers": (
            "<think>\ninternal scratchpad\n</think>\n"
            "Q1: Start with app flow. [load/loaders.py:70]\n"
            "Q2: Review staging lineage."
        ),
        "budget_summary": {
            "calls_per_model": {"qwen": 2},
            "estimated_tokens_per_model": {"qwen": 50},
        },
    }

    archivist.archive(
        module_graph,
        lineage_graph,
        semantic_results,
        git_velocity={},
    )

    codebase = (tmp_path / ".cartography" / "CODEBASE.md").read_text(encoding="utf-8")
    audit_text = (tmp_path / ".cartography" / "audit_trace.log").read_text(encoding="utf-8")
    onboarding = (tmp_path / ".cartography" / "onboarding_brief.md").read_text(encoding="utf-8")

    assert _is_pseudo("<dynamic>:temp") is True
    assert _is_macro("macros\\helper.sql") is True
    assert codebase.startswith("<!-- CARTOGRAPHER v1 | generated:")
    assert "## SECTION:COMPLETENESS_SCORE" in codebase
    assert "COMPLETENESS:" in codebase
    assert "edges_total=4" in codebase
    assert "macro_nodes=1" in codebase
    assert "## SECTION:ARCHITECTURE_SUMMARY" in codebase
    assert "## SECTION:KNOWN_DEBT" in codebase
    assert "cycles=1" in codebase
    assert "orphans=2" in codebase
    assert "## SECTION:HIGH_VELOCITY_FILES" in codebase
    assert "file=src/app.py|commits=6" in codebase
    assert "## SECTION:MACRO_INDEX" in codebase
    assert (
        "macro=normalize_city|source=macros/helpers.sql|line=1|args=name,country|"
        "purpose=Normalizes city names for downstream joins." in codebase
    )
    assert "## SECTION:MODULE_PURPOSE_INDEX" in codebase
    assert "module=src/app.py|purpose=Runs the pipeline" in codebase
    assert "## SECTION:LOW_CONFIDENCE_LINEAGE" in codebase
    assert (
        "edge=stg.orders->qa.orders_check|confidence=0.70|reason=jinja placeholders reduced certainty"
        in codebase
    )

    # ── Validate JSONL audit trace format ──────────────────────────
    import json

    lines = [line for line in audit_text.strip().splitlines() if line.strip()]
    assert len(lines) >= 1, "audit_trace.log should have at least one JSONL entry"

    required_keys = {"timestamp", "phase", "action", "confidence", "method", "evidence"}
    for line in lines:
        entry = json.loads(line)
        assert required_keys <= set(entry.keys()), f"Missing keys in: {entry}"
        assert entry["phase"] in {"surveyor", "hydrologist", "semanticist", "archivist"}
        if entry["confidence"] is not None:
            assert isinstance(entry["confidence"], int | float), (
                "confidence must be numeric or null"
            )
            assert 0.0 <= entry["confidence"] <= 1.0, (
                f"confidence out of range: {entry['confidence']}"
            )
        assert entry["method"] in ("static", "llm"), f"invalid method: {entry['method']}"
        if entry["evidence"] is not None:
            assert isinstance(entry["evidence"], str) and entry["evidence"], (
                "evidence must be non-empty when present"
            )
            assert ":" in entry["evidence"], (
                f"evidence should be file:line-like: {entry['evidence']}"
            )

    # Content-level checks on the JSONL entries
    all_actions = " ".join(json.loads(line_text)["action"] for line_text in lines)
    all_evidence = " ".join(json.loads(line_text)["evidence"] or "" for line_text in lines)
    assert "module_parsed" in all_actions
    assert "edge_added" in all_actions
    assert "drift_flagged" in all_actions
    assert "src/app.py" in all_evidence or "src\\app.py" in all_evidence

    answer_lines = [line for line in onboarding.splitlines() if line.startswith("Q")]
    assert answer_lines
    assert "<think>" not in onboarding
    assert "internal scratchpad" not in onboarding
    assert "Q1: Start with app flow." in onboarding
    assert "[UNVERIFIED: this file was not found in the analyzed graph]" in onboarding
    for answer_line in answer_lines:
        assert "[" in answer_line and "]" in answer_line


def test_archivist_onboarding_citations_use_real_metadata_and_correct_source(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")

    module_graph.graph.add_node(
        "load/loaders.py",
        source_file="load/loaders.py",
        source_line=35,
        file_type="python",
    )
    module_graph.graph.add_node(
        "tests/load/test_loaders.py",
        source_file="tests/load/test_loaders.py",
        source_line=12,
        file_type="python",
    )
    lineage_graph.graph.add_edge(
        "raw.seveso_2024",
        "seveso_2024",
        source_file="1_data/prepare/risques/sites_seveso.sql",
        source_line=None,
        line_range=[27, 50],
    )

    candidates = archivist._collect_evidence_candidates(module_graph, lineage_graph)

    q2 = "- `seveso_2024` is a critical output dataset."
    q3 = "- `tests/load/test_loaders.py` is in the blast radius."
    q5 = "Most frequent changes are in `load/loaders.py`."
    explicit = "Write to PostGIS via `load/loaders.py:148`."

    assert archivist._pick_evidence_for_line(q2, candidates) == (
        "`1_data/prepare/risques/sites_seveso.sql:27`"
    )
    assert archivist._pick_evidence_for_line(q3, candidates) == "`tests/load/test_loaders.py:12`"
    assert archivist._pick_evidence_for_line(q5, candidates) == "`load/loaders.py:35`"
    assert archivist._pick_evidence_for_line(explicit, candidates) == "`load/loaders.py:148`"


def test_archivist_module_purpose_index_uses_persisted_json(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    output_dir = tmp_path / ".cartography"
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "purpose_statements.json").write_text(
        '{"src/module_a.py": "Owns A flow."}', encoding="utf-8"
    )

    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")
    module_graph.graph.add_node("src/module_a.py", file_type="python")

    archivist.archive(module_graph, lineage_graph, semantic_results={}, git_velocity={})

    codebase = (output_dir / "CODEBASE.md").read_text(encoding="utf-8")
    assert "## SECTION:MODULE_PURPOSE_INDEX" in codebase
    assert "module=src/module_a.py|purpose=Owns A flow." in codebase


def test_archivist_onboarding_flags_unverified_cited_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")
    module_graph.graph.add_node("src/app.py", source_file="src/app.py", source_line=1)
    lineage_graph.graph.add_node("mart.orders")

    semantic_results = {
        "day_one_answers": "Q4: Logic is in macros. [macros/invented_macro.sql:12]",
    }

    archivist.archive(module_graph, lineage_graph, semantic_results, git_velocity={})

    onboarding = (tmp_path / ".cartography" / "onboarding_brief.md").read_text(encoding="utf-8")
    assert "[UNVERIFIED: this file was not found in the analyzed graph]" in onboarding
    assert "macros/invented_macro.sql:12" not in onboarding


def test_archivist_onboarding_keeps_dataset_node_citation_verified(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")

    module_graph.graph.add_node("src/app.py", source_file="src/app.py", source_line=1)
    lineage_graph.graph.add_node("infos_communes")

    semantic_results = {
        "day_one_answers": "Q2: Critical output dataset is infos_communes. [infos_communes:1]",
    }

    archivist.archive(module_graph, lineage_graph, semantic_results, git_velocity={})

    onboarding = (tmp_path / ".cartography" / "onboarding_brief.md").read_text(encoding="utf-8")
    assert "[infos_communes:1]" in onboarding
    assert "[UNVERIFIED: this file was not found in the analyzed graph]" not in onboarding


def test_archivist_onboarding_verifies_dataset_citation_by_lineage_substring(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")

    module_graph.graph.add_node("src/app.py", source_file="src/app.py", source_line=1)
    lineage_graph.graph.add_node(
        "make-open-data/1_data/prepare/foncier/ventes_immobilieres_enrichies.sql"
    )

    semantic_results = {
        "day_one_answers": "Q2: Key output. [ventes_immobilieres_enrichies:1]",
    }

    archivist.archive(module_graph, lineage_graph, semantic_results, git_velocity={})

    onboarding = (tmp_path / ".cartography" / "onboarding_brief.md").read_text(encoding="utf-8")
    assert "[ventes_immobilieres_enrichies:1]" in onboarding
    assert "[UNVERIFIED: this file was not found in the analyzed graph]" not in onboarding


def test_archivist_onboarding_verifies_dataset_name_inside_citation_prefix(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")

    module_graph.graph.add_node("src/app.py", source_file="src/app.py", source_line=1)
    lineage_graph.graph.add_node("ventes_immobilieres_enrichies")

    semantic_results = {
        "day_one_answers": "Q2: Critical output. [citation: ventes_immobilieres_enrichies:1]",
    }

    archivist.archive(module_graph, lineage_graph, semantic_results, git_velocity={})

    onboarding = (tmp_path / ".cartography" / "onboarding_brief.md").read_text(encoding="utf-8")
    assert "[citation: ventes_immobilieres_enrichies:1]" in onboarding
    assert "[UNVERIFIED: this file was not found in the analyzed graph]" not in onboarding


def test_archivist_onboarding_does_not_add_fallback_for_unmatched_explicit_file(
    monkeypatch, tmp_path
):
    monkeypatch.chdir(tmp_path)
    archivist = Archivist("repo")
    module_graph = KnowledgeGraph("module")
    lineage_graph = KnowledgeGraph("lineage")

    module_graph.graph.add_node("src/app.py", source_file="src/app.py", source_line=1)
    lineage_graph.graph.add_node("mart.orders")

    semantic_results = {
        "day_one_answers": "Q5: Most changed file is `uv.lock`.",
    }

    archivist.archive(module_graph, lineage_graph, semantic_results, git_velocity={})

    onboarding = (tmp_path / ".cartography" / "onboarding_brief.md").read_text(encoding="utf-8")
    assert "Q5: Most changed file is `uv.lock`." in onboarding
    assert "`src/app.py:1`" not in onboarding


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
    graph.graph.add_edge("raw.orders", "stg.orders", transformation_type="select")
    assert "src/app.py [python]" in _summarise_modules(graph)
    assert "raw.orders --[select]--> stg.orders" in _summarise_lineage(graph)

    captured = {}

    debug_messages = []

    def _fake_debug(message, payload):
        debug_messages.append((message, payload))

    monkeypatch.setattr("src.agents.semanticist.logger.debug", _fake_debug)

    def _fake_generate(model, prompt, timeout=300):
        captured["prompt"] = prompt
        return "Q1: answer"

    monkeypatch.setattr("src.agents.semanticist._ollama_generate", _fake_generate)
    answers = answer_day_one_questions(
        graph,
        {"src/app.py": "Purpose"},
        budget,
        {"src/app.py": 9},
        graph_intelligence={
            "critical_nodes": [
                {"node": "src/app.py", "score": 0.31, "in_degree": 3, "out_degree": 2},
                {"node": "mart.orders", "score": 0.21, "in_degree": 2, "out_degree": 0},
            ],
            "true_sources": ["raw.orders"],
            "true_sinks": ["mart.orders"],
            "blast_radius_top5": {"mart.orders": []},
            "cross_domain_risk": [
                {
                    "node": "infos_communes",
                    "domains": ["recensement", "geographie", "sante"],
                    "domain_count": 3,
                    "downstream_node_count": 4,
                }
            ],
            "high_velocity_files": [{"file": "src/app.py", "commits": 9}],
            "ingestion_pipeline": [
                {"file": "extract/source_to_storage.yml", "role": "extraction_config"}
            ],
            "macro_summary": {
                "macro_folder": "make-open-data/5_macros/",
                "macro_count": 3,
                "key_macros": [
                    {
                        "name": "geo_knn",
                        "source_file": "make-open-data/5_macros/foncier/geo_knn.sql",
                    },
                    {
                        "name": "aggreger_ventes_immobiliers",
                        "source_file": "make-open-data/5_macros/foncier/aggreger_ventes_immobiliers.sql",
                    },
                    {
                        "name": "pivoter_logement",
                        "source_file": "make-open-data/5_macros/recensement/pivoter_logement.sql",
                    },
                ],
            },
        },
    )
    assert answers == "Q1: answer"
    assert captured["prompt"].startswith("CRITICAL RULE: Only reference files, tables, datasets")
    assert "Output only the final answer for each question." in captured["prompt"]
    assert "=== CRITICAL_NODES ===" in captured["prompt"]
    assert "src/app.py [INFRASTRUCTURE - exclude from Q2]" in captured["prompt"]
    assert "mart.orders [DATASET - include in Q2]" in captured["prompt"]
    assert "=== BLAST_RADIUS_TOP5 ===" in captured["prompt"]
    assert "=== CROSS_DOMAIN_RISK ===" in captured["prompt"]
    assert "infos_communes | domains=recensement, geographie, sante" in captured["prompt"]
    assert "=== HIGH_VELOCITY_FILES ===" in captured["prompt"]
    assert "=== INGESTION_PIPELINE ===" in captured["prompt"]
    assert "Stage 1 - extraction_config: extract/source_to_storage.yml" in captured["prompt"]
    assert "=== MACRO_SUMMARY ===" in captured["prompt"]
    assert "macro_folder: make-open-data/5_macros/" in captured["prompt"]
    assert "For Q4, follow this exact logic:" in captured["prompt"]
    assert (
        "If macro_count is 0 or MACRO_SUMMARY is empty: do NOT mention macros at all."
        in captured["prompt"]
    )
    assert "For Q5, you MUST use only the HIGH_VELOCITY_FILES list to answer." in captured["prompt"]
    assert "Do NOT use PageRank scores or CRITICAL_NODES data for Q5." in captured["prompt"]
    assert (
        "For Q2, only list SQL dataset nodes and CSV seed files as critical outputs."
        in captured["prompt"]
    )
    assert "For Q3, follow this exact logic:" in captured["prompt"]
    assert "The node with the highest domain count is the most dangerous node" in captured["prompt"]
    assert debug_messages
    assert "graph_intelligence context before Day-One synthesis" in debug_messages[0][0]
    assert '"file": "src/app.py"' in debug_messages[0][1]
    assert '"commits": 9' in debug_messages[0][1]


def test_semanticist_q4_prompt_guard_when_macro_summary_empty(monkeypatch):
    graph = KnowledgeGraph("module")
    graph.graph.add_node("src/app.py", file_type="python")
    budget = ContextWindowBudget()
    captured = {}

    def _fake_generate(model, prompt, timeout=300):
        captured["prompt"] = prompt
        return "Q4: no macro layer detected in this repo."

    monkeypatch.setattr("src.agents.semanticist._ollama_generate", _fake_generate)

    answers = answer_day_one_questions(
        graph,
        {"src/app.py": "Purpose"},
        budget,
        {"src/app.py": 3},
        graph_intelligence={
            "critical_nodes": [
                {"node": "src/app.py", "score": 0.31, "in_degree": 1, "out_degree": 0}
            ],
            "true_sources": ["raw.orders"],
            "true_sinks": ["mart.orders"],
            "high_velocity_files": [{"file": "src/app.py", "commits": 3}],
            "macro_summary": {"macro_folder": "", "macro_count": 0, "key_macros": []},
        },
    )

    assert answers == "Q4: no macro layer detected in this repo."
    assert "=== MACRO_SUMMARY ===" in captured["prompt"]
    assert "  (none)" in captured["prompt"]
    assert (
        "If macro_count is 0 or MACRO_SUMMARY is empty: do NOT mention macros at all."
        in captured["prompt"]
    )
    assert "State clearly that no macro layer was detected in this repo." in captured["prompt"]


def test_uncertainty_scoring_values():
    from src.analyzers.sql_lineage import get_lineage_from_sql, strip_jinja

    clean_sql = "SELECT * FROM raw_orders"
    edges = get_lineage_from_sql(clean_sql, "stg_orders", "models/stg_orders.sql")
    assert edges[0].confidence == 0.95
    assert "direct SELECT" in edges[0].confidence_reason

    jinja_sql = "SELECT * FROM {{ ref('raw_orders') }} {% if true %} WHERE 1=1 {% endif %}"
    stripped = strip_jinja(jinja_sql)
    # The analyzer logic in analyze_sql_file handles the jinja_placeholder detection
    # but get_lineage_from_sql can be tested with a manual confidence
    edges_jinja = get_lineage_from_sql(
        stripped, "stg_orders", "models/stg_orders.sql", confidence=0.70, confidence_reason="jinja"
    )
    assert edges_jinja[0].confidence == 0.70
    assert edges_jinja[0].confidence_reason == "jinja"


def test_ollama_is_reachable_healthcheck(monkeypatch):
    class OkResponse:
        def raise_for_status(self):
            return None

    monkeypatch.setattr("src.agents.semanticist.httpx.get", lambda *args, **kwargs: OkResponse())
    assert ollama_is_reachable() is True

    def _raise(*args, **kwargs):
        raise RuntimeError("down")

    monkeypatch.setattr("src.agents.semanticist.httpx.get", _raise)
    assert ollama_is_reachable() is False


def test_semanticist_fast_fails_when_ollama_unavailable(monkeypatch, tmp_path):
    semanticist = Semanticist(str(tmp_path))
    graph = KnowledgeGraph("lineage")

    monkeypatch.setattr("src.agents.semanticist.ollama_is_reachable", lambda timeout=5: False)
    monkeypatch.setattr(
        Semanticist,
        "_collect_readable_modules",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("should not run")),
    )

    results = semanticist.analyse(graph)

    assert results["purpose_statements"] == {}
    assert results["drift_flags"] == {}
    assert results["domain_map"] == {}
    assert results["day_one_answers"] == ""
