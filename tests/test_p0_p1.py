"""
Tests for P0/P1 graph correctness improvements.
Covers: depends_on extraction, macro expansion, Python import edges,
and multi-directory macro merging.
"""
import os
import re
import tempfile
import pytest
from pathlib import Path

from src.analyzers.sql_lineage import (
    strip_jinja,
    analyze_sql_file,
    get_macros_map,
    analyze_all_sql_files,
)
from src.analyzers.tree_sitter_analyzer import PythonAnalyzer


# ---------------------------------------------------------------------------
# depends_on extraction
# ---------------------------------------------------------------------------

class TestDependsOnExtraction:
    """Verify that -- depends_on: {{ ref('...') }} comments create edges."""

    def test_single_depends_on(self, tmp_path):
        sql = tmp_path / "demo.sql"
        sql.write_text(
            "--- depends_on: {{ ref('logement_2020_valeurs') }}\n"
            "SELECT * FROM infos_communes\n",
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
        # Only the sql_select edge for my_table, no depends_on edges
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

        targets = {e.source_dataset for e in edges if e.transformation_type == "imports"}
        assert "mypkg/utils.py" in targets, (
            "Should create an import edge from mypkg/utils.py"
        )

    def test_no_edge_for_external_import(self, tmp_path):
        (tmp_path / "standalone.py").write_text(
            "import os\nimport sys\n",
            encoding="utf-8",
        )
        analyzer = PythonAnalyzer(str(tmp_path / "standalone.py"), str(tmp_path))
        _nodes, edges, imports = analyzer.analyze()

        import_edges = [e for e in edges if e.transformation_type == "imports"]
        assert len(import_edges) == 0, (
            "External stdlib imports should not produce import edges"
        )


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
