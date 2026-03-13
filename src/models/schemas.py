from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Base Nodes for the Graph
# ---------------------------------------------------------------------------


class GraphNode(BaseModel):
    """Base class for any node in our graphs."""

    id: str = Field(
        ..., description="Unique identifier for the node (e.g., file path or table name)"
    )
    source_file: str = Field(..., description="The YAML or SQL file where this node is defined")
    source_line: int | None = Field(None, description="Line number where the definition starts")
    description: str | None = Field("", description="A human-readable description of this node")


# ---------------------------------------------------------------------------
# Surveyor Schemas (Module/File Dependency Graph)
# ---------------------------------------------------------------------------


class ModuleNode(GraphNode):
    """
    Represents a file or module within the jaffle_shop repository.
    Used by the Surveyor to map out the physical files and their imports/references.
    """

    # e.g., 'sql', 'yaml', 'md'
    file_type: str = Field(..., description="The type of the file")

    # E.g. what dbt model name or source name this file establishes.
    # For a file like `models/customers.sql`, this might just be `customers`.
    logical_name: str | None = Field(
        None, description="The logical dbt name of the resource defined in this file"
    )


# ---------------------------------------------------------------------------
# Hydrologist Schemas (Data Lineage Graph)
# ---------------------------------------------------------------------------


class DatasetNode(GraphNode):
    """
    Represents a logical SQL table or view (a dbt model, source, or seed).
    Used by the Hydrologist to map data flow via standard SQL statements.
    """

    # e.g., 'model', 'source', 'seed'
    dataset_type: str = Field(
        ..., description="The type of dataset (e.g., dbt model, raw source, seed file)"
    )

    # The columns this dataset contains, if we can parse them from yaml or infer them.
    columns: list[str] = Field(
        default_factory=list, description="List of column names in this dataset"
    )
    column_descriptions: dict[str, str] = Field(
        default_factory=dict, description="Dictionary mapping column names to descriptions"
    )


class TransformationEdge(BaseModel):
    """
    Represents a directional dependency edge indicating data flows from one DatasetNode to another.
    E.g., if model B does `SELECT * FROM A`, there is a TransformationEdge from A -> B.
    """

    source_dataset: str = Field(
        ..., description="The ID of the DatasetNode supplying data (upstream)"
    )
    target_dataset: str = Field(
        ..., description="The ID of the DatasetNode consuming data (downstream)"
    )

    # Traceability
    source_file: str = Field(
        ..., description="The file where this transformation/dependency was discovered"
    )
    source_line: int | None = Field(
        None, description="The line number where this dependency was found"
    )

    # Type of transformation
    transformation_type: str = Field(
        "sql_select", description="The type of transformation (e.g., 'sql_select')"
    )

    # Confidence Score
    confidence: str = Field(
        "high", description="Confidence score for this edge ('high', 'medium', 'low', 'inferred')"
    )
