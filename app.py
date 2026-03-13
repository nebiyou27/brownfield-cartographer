import io
import json
import os
import sys
import tempfile
from contextlib import redirect_stdout

import networkx as nx
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network

# Add src folder to path for Orchestrator import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="Brownfield Cartographer", page_icon="🗺️", layout="wide")

CARTOGRAPHY_DIR = os.path.join(os.getcwd(), ".cartography")

# ───────────────── helpers ─────────────────


@st.cache_data(ttl=300)
def load_graph(path):
    """Load a NetworkX graph from a node-link JSON file."""
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    try:
        try:
            return nx.node_link_graph(data, edges="edges")
        except TypeError:
            return nx.node_link_graph(data)
    except Exception as e:
        st.error(f"Error loading graph: {e}")
        return None


def render_pyvis(nx_graph, height=700, physics=True, label_key="id"):
    """
    Convert a NetworkX graph to a Pyvis interactive HTML and display it.
    Node color is based on the 'dataset_type' or 'file_type' attribute.
    """
    COLOR_MAP = {
        "model": "#4CAF50",
        "source": "#2196F3",
        "seed": "#FF9800",
        "macro": "#9C27B0",
        "yaml": "#00BCD4",
        "python": "#F44336",
        "sql": "#8BC34A",
        "unknown": "#9E9E9E",
    }

    net = Network(
        height=f"{height}px", width="100%", directed=True, bgcolor="#1e1e2e", font_color="white"
    )
    net.toggle_physics(physics)

    for node_id, attrs in nx_graph.nodes(data=True):
        label = str(node_id)
        # choose category
        cat = (attrs.get("dataset_type") or attrs.get("file_type") or "unknown").lower()
        color = COLOR_MAP.get(cat, "#9E9E9E")
        title = f"<b>{label}</b><br>Type: {cat}"
        if "description" in attrs and attrs["description"]:
            desc = str(attrs["description"])[:200]
            title += f"<br>{desc}"
        orphaned = attrs.get("orphaned", False)
        border = "#FF0000" if orphaned else color
        net.add_node(str(node_id), label=label, color=color, border=border, title=title, size=20)

    for u, v, edata in nx_graph.edges(data=True):
        conf = edata.get("confidence", "")
        edge_label = conf if conf else ""
        edge_color = {
            "high": "#4CAF50",
            "medium": "#FF9800",
            "low": "#F44336",
            "inferred": "#9E9E9E",
        }.get(conf, "#aaaaaa")
        net.add_edge(str(u), str(v), title=edge_label, color=edge_color, arrows="to")

    with tempfile.NamedTemporaryFile(suffix=".html", delete=False, mode="w") as f:
        net.save_graph(f.name)
        tmp_path = f.name

    with open(tmp_path, encoding="utf-8") as f:
        html = f.read()
    os.unlink(tmp_path)
    components.html(html, height=height + 50, scrolling=False)


def run_orchestrator(target_dir, skip_semanticist):
    from src.orchestrator import Orchestrator

    buf = io.StringIO()
    with redirect_stdout(buf):
        try:
            orch = Orchestrator(target_dir, skip_semanticist=skip_semanticist)
            orch.run_analysis()
            return True, buf.getvalue()
        except Exception as e:
            return False, f"Error: {e}\n\n{buf.getvalue()}"


# ───────────────── UI ─────────────────

st.title("🗺️ Brownfield Cartographer")
st.markdown("Analyze and visualize codebase repositories and their data flows.")

# Sidebar
st.sidebar.header("⚙️ Configuration")
default_target = (
    "make-open-data" if os.path.exists(os.path.join(os.getcwd(), "make-open-data")) else ""
)
target_dir = st.sidebar.text_input("Target Directory", value=default_target)
skip_semanticist = st.sidebar.checkbox("Skip Semanticist (faster)", value=True)

if st.sidebar.button("▶ Run Analysis", type="primary"):
    if not target_dir or not os.path.exists(target_dir):
        st.sidebar.error("Target directory not found.")
    else:
        success, logs = run_orchestrator(target_dir, skip_semanticist)
        if success:
            st.sidebar.success("Analysis complete!")
        else:
            st.sidebar.error("Analysis failed.")
        with st.expander("📋 Analysis Logs"):
            st.code(logs)
        st.rerun()

# Load graphs
lineage_path = os.path.join(CARTOGRAPHY_DIR, "lineage_graph.json")
module_path = os.path.join(CARTOGRAPHY_DIR, "module_graph.json")
lineage_graph = load_graph(lineage_path)
module_graph = load_graph(module_path)

# ── Overview metrics ──
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Lineage Nodes", len(lineage_graph.nodes) if lineage_graph else "—")
with col2:
    st.metric("Lineage Edges", len(lineage_graph.edges) if lineage_graph else "—")
with col3:
    st.metric("Module Nodes", len(module_graph.nodes) if module_graph else "—")
with col4:
    st.metric("Module Edges", len(module_graph.edges) if module_graph else "—")

st.divider()

# ── Tabs ──
tab_lineage, tab_module, tab_data = st.tabs(
    ["🔗 Lineage Graph", "📦 Module Graph", "📋 Data Tables"]
)

with tab_lineage:
    if lineage_graph:
        st.subheader("Data Lineage Graph")

        lcol1, lcol2 = st.columns([3, 1])
        with lcol2:
            physics_l = st.checkbox("Physics simulation", value=True, key="phy_l")
            height_l = st.slider("Height (px)", 400, 1200, 700, key="h_l")

        with lcol1:
            render_pyvis(lineage_graph, height=height_l, physics=physics_l)

        # orphaned callout
        orphans = [n for n, d in lineage_graph.nodes(data=True) if d.get("orphaned")]
        if orphans:
            st.warning(f"⚠️ {len(orphans)} orphaned node(s): `{'`, `'.join(orphans)}`")
    else:
        st.info("No lineage graph found. Run analysis first.")

with tab_module:
    if module_graph:
        st.subheader("Module Dependency Graph")

        mcol1, mcol2 = st.columns([3, 1])
        with mcol2:
            physics_m = st.checkbox("Physics simulation", value=True, key="phy_m")
            height_m = st.slider("Height (px)", 400, 1200, 700, key="h_m")

        with mcol1:
            render_pyvis(module_graph, height=height_m, physics=physics_m)
    else:
        st.info("No module graph found. Run analysis first.")

with tab_data:
    st.subheader("Raw Node Data")
    dt1, dt2 = st.tabs(["Lineage Nodes", "Module Nodes"])

    with dt1:
        if lineage_graph:
            rows = [{"id": n, **d} for n, d in lineage_graph.nodes(data=True)]
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        else:
            st.info("No lineage data.")

    with dt2:
        if module_graph:
            rows = [{"id": n, **d} for n, d in module_graph.nodes(data=True)]
            df = pd.DataFrame(rows)
            # drop very long list columns for readability
            list_cols = [c for c in df.columns if df[c].apply(lambda x: isinstance(x, list)).any()]
            st.dataframe(df.drop(columns=list_cols, errors="ignore"), use_container_width=True)
        else:
            st.info("No module data.")
