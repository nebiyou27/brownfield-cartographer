"""
Tests for P0/P1 graph correctness improvements.
Covers: depends_on extraction, macro expansion, Python import edges,
and multi-directory macro merging.
"""

from src.analyzers.sql_lineage import (
    SQLLineageAnalyzer,
    analyze_sql_file,
    get_macros_map,
    strip_jinja,
)
from src.analyzers.tree_sitter_analyzer import PythonAnalyzer
from src.graph.knowledge_graph import KnowledgeGraph

# ---------------------------------------------------------------------------
# depends_on extraction
# ---------------------------------------------------------------------------


class TestDependsOnExtraction:
    """Verify that -- depends_on: {{ ref('...') }} comments create edges."""

    def test_single_depends_on(self, tmp_path):
        sql = tmp_path / "demo.sql"
        sql.write_text(
            "--- depends_on: {{ ref('logement_2020_valeurs') }}\nSELECT * FROM infos_communes\n",
            encoding="utf-8",
        )
        edges = analyze_sql_file(str(sql))
        sources = {e.source_dataset for e in edges}
        assert "logement_2020_valeurs" in sources, (
            "depends_on comment should produce an edge from logement_2020_valeurs"
        )

    def test_multiple_depends_on(self, tmp_path):
        sql = tmp_path / "multi.sql"
        sql.write_text(
            "--- depends_on: {{ ref('logement_2020_valeurs') }}\n"
            "  -- depends_on: {{ ref('habitat_renomee') }}\n"
            "SELECT * FROM infos_communes\n",
            encoding="utf-8",
        )
        edges = analyze_sql_file(str(sql))
        sources = {e.source_dataset for e in edges}
        assert "logement_2020_valeurs" in sources
        assert "habitat_renomee" in sources

    def test_no_depends_on(self, tmp_path):
        sql = tmp_path / "plain.sql"
        sql.write_text("SELECT * FROM my_table\n", encoding="utf-8")
        edges = analyze_sql_file(str(sql))
        # Only the SELECT lineage edge for my_table, no depends_on edges
        assert all(e.source_dataset != "logement_2020_valeurs" for e in edges)


# ---------------------------------------------------------------------------
# aggreger_supra_commune macro expansion
# ---------------------------------------------------------------------------


class TestMacroExpansion:
    """Verify that aggreger_supra_commune is expanded before Jinja is stripped."""

    def test_macro_expanded_to_union(self):
        raw = "SELECT * FROM ({{ aggreger_supra_commune('mobilite', 'code_departement') }}) t"
        cleaned = strip_jinja(raw)
        assert "mobilite_communes" in cleaned
        assert "logement_2020_valeurs" in cleaned
        assert "jinja_placeholder" not in cleaned

    def test_unknown_macro_becomes_placeholder(self):
        raw = "SELECT * FROM {{ some_other_macro('arg') }}"
        cleaned = strip_jinja(raw)
        assert "jinja_placeholder" in cleaned


# ---------------------------------------------------------------------------
# Python import edges
# ---------------------------------------------------------------------------


class TestPythonImportEdges:
    """Verify that PythonAnalyzer emits import edges for internal modules."""

    def test_import_edge_created(self, tmp_path):
        # Create a fake repo with two Python files
        pkg = tmp_path / "mypkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("", encoding="utf-8")
        (pkg / "utils.py").write_text("def helper(): pass\n", encoding="utf-8")
        (pkg / "main.py").write_text(
            "from mypkg.utils import helper\n\ndef run(): helper()\n",
            encoding="utf-8",
        )

        analyzer = PythonAnalyzer(str(pkg / "main.py"), str(tmp_path))
        _nodes, edges, imports = analyzer.analyze()

        targets = {e.source_dataset for e in edges if e.transformation_type == "config"}
        assert "mypkg/utils.py" in targets, "Should create an import edge from mypkg/utils.py"

    def test_no_edge_for_external_import(self, tmp_path):
        (tmp_path / "standalone.py").write_text(
            "import os\nimport sys\n",
            encoding="utf-8",
        )
        analyzer = PythonAnalyzer(str(tmp_path / "standalone.py"), str(tmp_path))
        _nodes, edges, imports = analyzer.analyze()

        import_edges = [e for e in edges if e.transformation_type == "config"]
        assert len(import_edges) == 0, "External stdlib imports should not produce import edges"


# ---------------------------------------------------------------------------
# Multi-directory macro merging
# ---------------------------------------------------------------------------


class TestMultiMacroDirMerging:
    """Verify that macros from multiple directories are merged."""

    def test_macros_merged(self, tmp_path):
        dir_a = tmp_path / "macros_a"
        dir_a.mkdir()
        (dir_a / "alpha.sql").write_text(
            "{% macro alpha_macro(x) %} SELECT 1 {% endmacro %}",
            encoding="utf-8",
        )

        dir_b = tmp_path / "macros_b"
        dir_b.mkdir()
        (dir_b / "beta.sql").write_text(
            "{% macro beta_macro(x) %} SELECT 2 {% endmacro %}",
            encoding="utf-8",
        )

        map_a = get_macros_map(str(dir_a))
        map_b = get_macros_map(str(dir_b))
        merged = {**map_a, **map_b}

        assert "alpha_macro" in merged
        assert "beta_macro" in merged


class TestSQLDialectDetection:
    """Verify multi-dialect parsing and dialect selection logging."""

    def test_default_dialect_order_is_exposed(self):
        analyzer = SQLLineageAnalyzer()
        assert analyzer.dialect_order == ["duckdb", "postgres", "bigquery", "snowflake"]

    def test_auto_detects_dialect_and_logs(self, monkeypatch):
        import sqlglot
        from sqlglot.errors import ParseError

        from src.analyzers import sql_lineage

        original_parse = sqlglot.parse
        info_messages = []

        def fake_parse(sql, read=None):
            if read == "duckdb":
                raise ParseError("duckdb fail")
            return original_parse(sql, read="postgres")

        def fake_info(message, *args):
            info_messages.append(message % args)

        monkeypatch.setattr(sqlglot, "parse", fake_parse)
        monkeypatch.setattr(sql_lineage.logger, "info", fake_info)
        analyzer = SQLLineageAnalyzer(["duckdb", "postgres"])

        edges = analyzer.get_lineage_from_sql(
            "SELECT * FROM raw_orders",
            "stg_orders",
            "models/stg_orders.sql",
        )

        assert edges
        assert any("Selected dialect 'postgres'" in msg for msg in info_messages)

    def test_reports_best_candidate_when_all_dialects_fail(self, monkeypatch):
        import sqlglot

        from src.analyzers import sql_lineage

        class MultiError(Exception):
            def __init__(self, count):
                super().__init__(f"{count} errors")
                self.errors = [object() for _ in range(count)]

        warning_messages = []

        def fake_parse(sql, read=None):
            counts = {"duckdb": 3, "postgres": 1, "bigquery": 2, "snowflake": 4}
            raise MultiError(counts[read])

        def fake_warning(message, *args):
            warning_messages.append(message % args)

        monkeypatch.setattr(sqlglot, "parse", fake_parse)
        monkeypatch.setattr(sql_lineage.logger, "warning", fake_warning)
        analyzer = SQLLineageAnalyzer(["duckdb", "postgres", "bigquery", "snowflake"])

        edges = analyzer.get_lineage_from_sql("SELECT 1", "target", "models/t.sql")

        assert edges == []
        assert any("Best candidate by parse errors: 'postgres'" in msg for msg in warning_messages)


class TestEnhancedPythonDataFlowDetection:
    def test_detects_requested_pandas_pyspark_sqlalchemy_patterns(self, tmp_path):
        py_file = tmp_path / "flow.py"
        py_file.write_text(
            "\n".join(
                [
                    "import pandas as pd",
                    "df = pd.read_csv('in.csv')",
                    "pd.read_parquet('in.parquet')",
                    "pd.read_sql('select 1')",
                    "pd.read_excel('in.xlsx')",
                    "df.to_csv('out.csv')",
                    "df.to_parquet('out.parquet')",
                    "df.to_excel('out.xlsx')",
                    "spark.read.csv('spark_in.csv')",
                    "df.write.csv('spark_out.csv')",
                    "spark.sql('select 1')",
                    "engine.execute('select 1')",
                    "session.query('table')",
                    "engine.connect('db://conn')",
                ]
            ),
            encoding="utf-8",
        )

        analyzer = PythonAnalyzer(str(py_file), str(tmp_path))
        _nodes, edges, _imports = analyzer.analyze()

        by_pair = {(e.source_dataset, e.target_dataset, e.transformation_type) for e in edges}
        assert ("in.csv", "flow.py", "python_read") in by_pair
        assert ("in.parquet", "flow.py", "python_read") in by_pair
        assert ("in.xlsx", "flow.py", "python_read") in by_pair
        assert ("flow.py", "out.csv", "python_write") in by_pair
        assert ("flow.py", "out.parquet", "python_write") in by_pair
        assert ("flow.py", "out.xlsx", "python_write") in by_pair
        assert ("spark_in.csv", "flow.py", "python_read") in by_pair
        assert ("flow.py", "spark_out.csv", "python_write") in by_pair

    def test_logs_warning_for_unresolved_dynamic_references(self, tmp_path, monkeypatch):
        from src.analyzers import tree_sitter_analyzer

        py_file = tmp_path / "dynamic_refs.py"
        py_file.write_text(
            "\n".join(
                [
                    "path = '/tmp/data.csv'",
                    "import pandas as pd",
                    "df = pd.read_csv(path)",
                    "spark.sql(query_text)",
                    "engine.execute(sql_stmt)",
                ]
            ),
            encoding="utf-8",
        )

        warnings = []

        def fake_warning(message, *args):
            warnings.append(message % args)

        monkeypatch.setattr(tree_sitter_analyzer.logger, "warning", fake_warning)

        analyzer = PythonAnalyzer(str(py_file), str(tmp_path))
        analyzer.analyze()

        assert any("dynamic_refs.py:3" in msg for msg in warnings)
        assert any("dynamic_refs.py:4" in msg for msg in warnings)
        assert any("dynamic_refs.py:5" in msg for msg in warnings)


class TestEdgeMetadata:
    ALLOWED = {"select", "join", "cte", "insert", "python_read", "python_write", "config"}

    def test_sql_edges_have_metadata(self, tmp_path):
        sql = tmp_path / "meta.sql"
        sql.write_text(
            "WITH cte1 AS (SELECT * FROM raw_orders)\nSELECT * FROM cte1 JOIN raw_customers ON 1=1\n",
            encoding="utf-8",
        )

        edges = analyze_sql_file(str(sql))
        assert edges
        for edge in edges:
            assert edge.source_file
            assert edge.transformation_type in self.ALLOWED
            assert edge.line_range is not None
            assert len(edge.line_range) == 2

    def test_python_edges_have_metadata(self, tmp_path):
        py_file = tmp_path / "meta.py"
        py_file.write_text("import pandas as pd\npd.read_csv('x.csv')\n", encoding="utf-8")

        analyzer = PythonAnalyzer(str(py_file), str(tmp_path))
        _nodes, edges, _imports = analyzer.analyze()

        assert edges
        for edge in edges:
            assert edge.source_file
            assert edge.transformation_type in self.ALLOWED
            assert edge.line_range is not None
            assert len(edge.line_range) == 2


class TestKnowledgeGraphBlastRadius:
    def test_blast_radius_returns_downstream_confidence_reason(self):
        graph = KnowledgeGraph("lineage")
        graph.graph.add_edge(
            "raw.orders",
            "stg.orders",
            confidence=0.95,
            confidence_reason="sql parse",
        )
        graph.graph.add_edge(
            "stg.orders",
            "mart.orders",
            confidence=0.70,
            confidence_reason="jinja placeholders reduced certainty",
        )

        impact = graph.blast_radius("raw.orders")

        assert isinstance(impact, list)
        assert {item["node"] for item in impact} == {"stg.orders", "mart.orders"}
        mart = next(item for item in impact if item["node"] == "mart.orders")
        assert mart["path_confidence"] == 0.70
        assert mart["reason"] == "jinja placeholders reduced certainty"
