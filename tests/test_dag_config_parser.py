from src.analyzers import dag_config_parser as parser


def test_parse_yaml_file_handles_tabs_and_invalid_yaml(tmp_path):
    valid = tmp_path / "valid.yml"
    valid.write_text("name:\tproject\n", encoding="utf-8")

    parsed, error = parser.parse_yaml_file(str(valid))

    assert parsed == {"name": "project"}
    assert error is None

    invalid = tmp_path / "invalid.yml"
    invalid.write_text("models:\n  - name: [\n", encoding="utf-8")

    parsed, error = parser.parse_yaml_file(str(invalid))

    assert parsed == {}
    assert "yaml parse failure" in error


def test_load_doc_blocks_and_resolve_references(tmp_path):
    docs = tmp_path / "docs.md"
    docs.write_text(
        "{% docs customers %}Customer description{% enddocs %}\n"
        "{% docs status_desc %}Status column text{% enddocs %}\n",
        encoding="utf-8",
    )

    doc_blocks = parser.load_doc_blocks(str(tmp_path))

    assert doc_blocks == {
        "customers": "Customer description",
        "status_desc": "Status column text",
    }
    assert (
        parser.resolve_doc_references('{{ doc("customers") }}', doc_blocks)
        == "Customer description"
    )
    assert (
        parser.resolve_doc_references("prefix {{ doc('status_desc') }} suffix", doc_blocks)
        == "prefix Status column text suffix"
    )


def test_extract_nodes_from_schema_yml_builds_model_and_source_nodes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    schema = tmp_path / "models" / "schema.yml"
    schema.parent.mkdir()
    schema.write_text(
        """
models:
  - name: orders
    description: "{{ doc('orders_doc') }}"
    columns:
      - name: status
        description: "{{ doc('status_doc') }}"
sources:
  - name: raw
    description: "{{ doc('source_doc') }}"
    tables:
      - name: raw_orders
        columns:
          - name: id
            description: "Primary key"
""",
        encoding="utf-8",
    )

    doc_blocks = {
        "orders_doc": "Order facts",
        "status_doc": "Current status",
        "source_doc": "Raw source",
    }
    nodes = parser.extract_nodes_from_schema_yml(str(schema), doc_blocks)

    assert [node.id for node in nodes] == ["orders", "raw_orders"]
    assert nodes[0].description == "Order facts"
    assert nodes[0].column_descriptions == {"status": "Current status"}
    assert nodes[1].dataset_type == "source"
    assert nodes[1].description == "Raw source"
    assert nodes[1].column_descriptions == {"id": "Primary key"}


def test_extract_nodes_from_schema_yml_returns_parse_failure_node(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    broken = tmp_path / "broken.yml"
    broken.write_text("models:\n  - name: [\n", encoding="utf-8")

    nodes = parser.extract_nodes_from_schema_yml(str(broken))

    assert len(nodes) == 1
    failure = nodes[0]
    assert failure.parsed is False
    assert "yaml parse failure" in failure.reason


def test_extract_project_metadata_and_analyze_all_yaml_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    project = tmp_path / "dbt_project.yml"
    project.write_text(
        """
name: cartographer
model-paths: ["warehouse/models"]
seed-paths: ["warehouse/seeds"]
macro-paths: ["warehouse/macros"]
""",
        encoding="utf-8",
    )

    metadata = parser.extract_project_metadata(str(project))
    results = parser.analyze_all_yaml_files(str(tmp_path))

    assert metadata["node"].logical_name == "cartographer"
    assert metadata["model_paths"] == ["warehouse/models"]
    assert metadata["seed_paths"] == ["warehouse/seeds"]
    assert metadata["macro_paths"] == ["warehouse/macros"]
    assert results["project"].id == "cartographer"
    assert results["model_paths"] == ["warehouse/models"]
    assert results["seed_paths"] == ["warehouse/seeds"]
    assert results["macro_paths"] == ["warehouse/macros"]
    assert results["datasets"] == []
