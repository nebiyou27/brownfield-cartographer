"""
Tree-sitter based analyzer for Python files.
Extracts data flow patterns (pandas I/O, SQLAlchemy, psycopg) and import statements.
"""

import glob
import os
import re
from pathlib import Path

import tree_sitter_python as tspython
from tree_sitter import Language, Parser

from ..logger import get_logger
from ..models.schemas import DatasetNode, ModuleNode, TransformationEdge
from ..path_utils import normalize_path_key

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Tree-sitter setup
# ---------------------------------------------------------------------------

PY_LANGUAGE = Language(tspython.language())
_parser = Parser(PY_LANGUAGE)

# ---------------------------------------------------------------------------
# Patterns we look for in Python files
# ---------------------------------------------------------------------------

# pandas read functions  →  CONSUMES
PANDAS_READ_PATTERNS = {
    "read_csv",
    "read_json",
    "read_sql",
    "read_excel",
    "read_parquet",
    "read_sql_query",
    "read_sql_table",
}

# pandas/geopandas write functions  →  PRODUCES
PANDAS_WRITE_PATTERNS = {
    "to_csv",
    "to_sql",
    "to_parquet",
    "to_excel",
    "to_postgis",
}

# SQLAlchemy / psycopg patterns
SQLALCHEMY_PATTERNS = {"create_engine"}
DB_EXECUTE_PATTERNS = {"execute"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _node_text(node) -> str:
    """Return the UTF-8 text of a tree-sitter node."""
    return node.text.decode("utf-8")


def _is_dynamic(text: str) -> bool:
    """Check if a string argument is dynamic (f-string, variable, concat)."""
    if text.startswith('f"') or text.startswith("f'"):
        return True
    # No quotes at all → it's a variable reference
    if not (text.startswith("'") or text.startswith('"')):
        return True
    return False


def _extract_string_value(text: str) -> str | None:
    """Extract the literal string value, stripping quotes."""
    text = text.strip()
    for q in ('"""', "'''", '"', "'"):
        if text.startswith(q) and text.endswith(q):
            return text[len(q) : -len(q)]
    return None


# ---------------------------------------------------------------------------
# PythonAnalyzer
# ---------------------------------------------------------------------------


class PythonAnalyzer:
    """
    Analyzes a single Python file using tree-sitter to extract:
    - Module-level imports
    - Data source reads  (CONSUMES edges)
    - Data sink writes   (PRODUCES edges)
    - Dynamic / unresolvable references (logged as warnings)
    """

    def __init__(self, file_path: str, repo_root: str):
        self.file_path = file_path
        self.repo_root = repo_root
        try:
            self.rel_path = normalize_path_key(str(Path(file_path).relative_to(Path(repo_root))))
        except ValueError:
            self.rel_path = normalize_path_key(file_path)

        self.imports: list[str] = []
        self.nodes: list[DatasetNode] = []
        self.edges: list[TransformationEdge] = []

    # ---- public API --------------------------------------------------------

    def analyze(self) -> tuple[list[DatasetNode], list[TransformationEdge], list[str]]:
        """
        Parse the file and return (nodes, edges, imports).
        """
        try:
            with open(self.file_path, encoding="utf-8") as f:
                source = f.read()
        except Exception as e:
            logger.warning(f"Could not read {self.file_path}: {e}")
            return [], [], []

        tree = _parser.parse(bytes(source, "utf-8"))
        root = tree.root_node

        self._extract_imports(root)
        self._extract_calls(root)
        self._extract_orchestration(root)

        return self.nodes, self.edges, self.imports

    # ---- import extraction -------------------------------------------------

    def _extract_imports(self, root):
        """Extract module-level import statements."""
        for child in root.children:
            text = _node_text(child)
            if child.type == "import_statement":
                self.imports.append(text)
                match = re.search(r"^import\s+([a-zA-Z0-9_\.]+)", text)
                if match:
                    self._check_and_add_import_edge(match.group(1), child)
            elif child.type == "import_from_statement":
                self.imports.append(text)
                match = re.search(r"^from\s+([a-zA-Z0-9_\.]+)\s+import", text)
                if match:
                    self._check_and_add_import_edge(match.group(1), child)

    def _check_and_add_import_edge(self, module_path_str: str, node):
        """Check if the imported module exists in the repo and add an edge."""
        parts = module_path_str.split(".")
        # Check both file.py and dir/__init__.py
        rel_paths_to_check = [os.path.join(*parts) + ".py", os.path.join(*parts, "__init__.py")]

        for rel_check in rel_paths_to_check:
            abs_path = os.path.join(self.repo_root, rel_check)
            if os.path.exists(abs_path):
                line_no = node.start_point[0] + 1
                source_id = Path(rel_check).as_posix()
                target_id = Path(self.rel_path).as_posix()

                edge = TransformationEdge(
                    source_dataset=source_id,
                    target_dataset=target_id,
                    source_file=target_id,
                    source_line=line_no,
                    transformation_type="imports",
                    confidence=0.95,
                    confidence_reason=f"structural Python import of '{module_path_str}'",
                )
                self.edges.append(edge)
                break

    # ---- call extraction ---------------------------------------------------

    def _extract_calls(self, root):
        """Walk the AST and find relevant function calls."""
        self._walk(root)

    def _walk(self, node):
        if node.type == "call":
            self._process_call(node)
        for child in node.children:
            self._walk(child)

    def _process_call(self, call_node):
        """Inspect a call node and decide if it's a data-flow call."""
        func_node = call_node.child_by_field_name("function")
        if func_node is None:
            return

        func_text = _node_text(func_node)

        # --- pandas / geopandas reads: pd.read_csv(...) --------------------
        for pattern in PANDAS_READ_PATTERNS:
            if func_text.endswith(pattern):
                self._handle_data_call(
                    call_node,
                    pattern,
                    "consumes",
                    dataset_type="file_source",
                )
                return

        # --- pandas / geopandas writes: df.to_csv(...) ---------------------
        for pattern in PANDAS_WRITE_PATTERNS:
            if func_text.endswith(pattern):
                self._handle_data_call(
                    call_node,
                    pattern,
                    "produces",
                    dataset_type="file_sink",
                )
                return

        # --- SQLAlchemy create_engine(...) ----------------------------------
        if func_text.endswith("create_engine"):
            self._handle_data_call(
                call_node,
                "create_engine",
                "consumes",
                dataset_type="database",
            )
            return

        # --- cursor.execute(...) / connection.execute(...) ------------------
        if func_text.endswith("execute"):
            self._handle_data_call(
                call_node,
                "execute",
                "consumes",
                dataset_type="database_query",
            )
            return

    def _handle_data_call(
        self,
        call_node,
        call_name: str,
        edge_type: str,  # "produces" or "consumes"
        dataset_type: str = "unknown",
    ):
        """
        Extract the first positional argument of a data call.
        If it is a literal string, create a DatasetNode + edge.
        If it is dynamic, log a warning.
        """
        args_node = call_node.child_by_field_name("arguments")
        if args_node is None:
            return

        # Find the first positional argument
        first_arg = None
        for child in args_node.children:
            if child.type not in ("(", ")", ",", "keyword_argument"):
                first_arg = child
                break

        if first_arg is None:
            return

        arg_text = _node_text(first_arg)
        line_no = call_node.start_point[0] + 1  # 1-indexed

        if _is_dynamic(arg_text):
            logger.warning(
                "Unresolved dynamic reference in %s:%d — %s(%s)",
                self.rel_path,
                line_no,
                call_name,
                arg_text,
            )
            # Still create an edge with a placeholder so the graph is complete
            dataset_id = f"<dynamic>:{call_name}:{self.rel_path}:{line_no}"
            confidence = 0.60
            reason = f"inferred from dynamic {call_name}() call (f-string or variable)"
        else:
            literal = _extract_string_value(arg_text)
            if literal is None:
                return

            # Filter ephemeral SQL commands
            if call_name == "execute":
                upper_lit = literal.strip().upper()
                if upper_lit.startswith(
                    ("INSTALL", "LOAD", "DROP", "SET", "PRAGMA", "VACUUM", "CHECKPOINT", "CALL")
                ):
                    return

            dataset_id = literal
            confidence = 0.75
            reason = f"inferred from literal argument in {call_name}() call"

        # Create a DatasetNode for the source/sink
        ds_node = DatasetNode(
            id=dataset_id,
            source_file=self.rel_path,
            source_line=line_no,
            description=f"{call_name}() call",
            dataset_type=dataset_type,
        )
        self.nodes.append(ds_node)

        # Create the edge
        if edge_type == "consumes":
            edge = TransformationEdge(
                source_dataset=dataset_id,
                target_dataset=self.rel_path,
                source_file=self.rel_path,
                source_line=line_no,
                transformation_type="consumes",
                confidence=confidence,
                confidence_reason=reason,
            )
        else:  # produces
            edge = TransformationEdge(
                source_dataset=self.rel_path,
                target_dataset=dataset_id,
                source_file=self.rel_path,
                source_line=line_no,
                transformation_type="produces",
                confidence=confidence,
                confidence_reason=reason,
            )
        self.edges.append(edge)

    # ---- orchestration extraction ------------------------------------------

    def _extract_orchestration(self, root):
        """Walk the AST and find Dagster @asset / @op functions."""
        self._walk_orchestration(root)

    def _walk_orchestration(self, node):
        if node.type == "decorated_definition":
            self._process_decorated(node)
        for child in node.children:
            self._walk_orchestration(child)

    def _process_decorated(self, node):
        # find the decorator
        is_dagster = False
        for child in node.children:
            if child.type == "decorator":
                decorator_text = _node_text(child)
                if "@asset" in decorator_text or "@op" in decorator_text:
                    is_dagster = True
                    break

        if not is_dagster:
            return

        # find the function definition
        func_def = None
        for child in node.children:
            if child.type == "function_definition":
                func_def = child
                break

        if not func_def:
            return

        # find function name
        name_node = func_def.child_by_field_name("name")
        if not name_node:
            return

        asset_name = _node_text(name_node)
        line_no = name_node.start_point[0] + 1

        # Add a node for the asset
        ds_node = DatasetNode(
            id=asset_name,
            source_file=self.rel_path,
            source_line=line_no,
            description="Dagster orchestrator node",
            dataset_type="orchestration",
        )
        self.nodes.append(ds_node)

        # find parameters
        params_node = func_def.child_by_field_name("parameters")
        if params_node:
            for param in params_node.children:
                param_name = None
                if param.type == "identifier":
                    param_name = _node_text(param)
                elif param.type == "typed_parameter":
                    id_node = param.child(0)
                    if id_node and id_node.type == "identifier":
                        param_name = _node_text(id_node)

                if param_name and param_name not in ("context", "self"):
                    edge = TransformationEdge(
                        source_dataset=param_name,
                        target_dataset=asset_name,
                        source_file=self.rel_path,
                        source_line=param.start_point[0] + 1,
                        transformation_type="depends_on",
                        confidence=0.95,
                        confidence_reason=f"explicit Dagster orchestration dependency in '{asset_name}'",
                    )
                    self.edges.append(edge)


# ---------------------------------------------------------------------------
# LanguageRouter
# ---------------------------------------------------------------------------


class LanguageRouter:
    """
    Routes source files to the appropriate language-specific analyzer.
    Currently supports Python (.py) files.
    """

    EXTENSION_MAP = {
        ".py": "python",
    }

    def __init__(self, repo_root: str):
        self.repo_root = repo_root

    def analyze_file(
        self, file_path: str
    ) -> tuple[
        ModuleNode | None,
        list[DatasetNode],
        list[TransformationEdge],
    ]:
        """
        Analyze a single file and return (module_node, dataset_nodes, edges).
        Returns (None, [], []) for unsupported file types.
        """
        ext = Path(file_path).suffix.lower()
        lang = self.EXTENSION_MAP.get(ext)

        if lang is None:
            return None, [], []

        if lang == "python":
            return self._analyze_python(file_path)

        return None, [], []

    def analyze_directory(
        self, directory: str
    ) -> tuple[
        list[ModuleNode],
        list[DatasetNode],
        list[TransformationEdge],
    ]:
        """
        Recursively analyze all supported files in a directory.
        """
        all_modules: list[ModuleNode] = []
        all_datasets: list[DatasetNode] = []
        all_edges: list[TransformationEdge] = []

        py_files = glob.glob(os.path.join(directory, "**", "*.py"), recursive=True)

        for file_path in py_files:
            logger.info(f"[Tree-sitter] Processing: {file_path}")
            mod_node, datasets, edges = self.analyze_file(file_path)
            if mod_node:
                all_modules.append(mod_node)
            all_datasets.extend(datasets)
            all_edges.extend(edges)

        return all_modules, all_datasets, all_edges

    # ---- private -----------------------------------------------------------

    def _analyze_python(
        self, file_path: str
    ) -> tuple[
        ModuleNode | None,
        list[DatasetNode],
        list[TransformationEdge],
    ]:
        try:
            rel_path = normalize_path_key(str(Path(file_path).relative_to(Path(self.repo_root))))
        except ValueError:
            rel_path = normalize_path_key(file_path)

        analyzer = PythonAnalyzer(file_path, self.repo_root)
        datasets, edges, imports = analyzer.analyze()

        # Create a ModuleNode for the file itself
        mod_node = ModuleNode(
            id=rel_path,
            source_file=rel_path,
            file_type="python",
            logical_name=Path(file_path).stem,
            description=f"Python module with {len(imports)} imports, {len(edges)} data-flow edges",
        )

        return mod_node, datasets, edges
