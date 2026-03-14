import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import urlparse

from .logger import get_logger
from .orchestrator import Orchestrator

logger = get_logger(__name__)


def _is_github_url(value: str) -> bool:
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https"):
        return False
    return parsed.netloc.lower() == "github.com" and bool(parsed.path.strip("/"))


def main():
    """
    Main entry point for the Brownfield Cartographer CLI.

    Subcommands:
        analyze <repo_path>              — full pipeline analysis
        query   <repo_path> [--ask Q]   — interactive or single-shot Navigator
    """
    parser = argparse.ArgumentParser(
        description="Brownfield Cartographer: A tool for mapping modular dependencies and data lineage."
    )

    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # ── analyze ──────────────────────────────────────────────────────────
    analyze_parser = subparsers.add_parser(
        "analyze", help="Run the full analysis pipeline on a repository"
    )
    analyze_parser.add_argument(
        "repo_path", help="Path to the repository to analyze (local path or GitHub URL)"
    )
    analyze_parser.add_argument(
        "--no-semanticist",
        action="store_true",
        default=False,
        help="Skip the Semanticist agent (faster iteration, no LLM calls)",
    )
    analyze_parser.add_argument(
        "--incremental",
        action="store_true",
        default=False,
        help="Incremental mode: only re-analyze files changed since last saved commit state.",
    )

    # ── query ─────────────────────────────────────────────────────────────
    query_parser = subparsers.add_parser(
        "query", help="Query the knowledge graph interactively via the Navigator agent"
    )
    query_parser.add_argument(
        "repo_path", help="Path to the repository whose .cartography/ artifacts to query"
    )
    query_parser.add_argument(
        "--ask",
        metavar="QUESTION",
        default=None,
        help="Run a single query and exit (non-interactive). "
        "e.g. --ask 'What produces ventes_immobilieres?'",
    )

    args = parser.parse_args()

    # ── analyze handler ───────────────────────────────────────────────────
    if args.command == "analyze":
        temp_repo_root = None
        try:
            if _is_github_url(args.repo_path):
                temp_repo_root = tempfile.mkdtemp(prefix="cartography_clone_")
                target_path = os.path.join(temp_repo_root, "repo")
                logger.info("Cloning GitHub repository: %s", args.repo_path)
                subprocess.run(
                    ["git", "clone", args.repo_path, target_path],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            else:
                target_path = os.path.abspath(args.repo_path)
                if not os.path.exists(target_path):
                    logger.error("Repository path does not exist: %s", target_path)
                    sys.exit(1)

            orchestrator = Orchestrator(
                target_path,
                skip_semanticist=args.no_semanticist,
                incremental=args.incremental,
            )
            orchestrator.run_analysis()
        except subprocess.CalledProcessError as e:
            logger.error("Failed to clone repository: %s", args.repo_path)
            if e.stderr:
                logger.error("%s", e.stderr.strip())
            sys.exit(1)
        finally:
            if temp_repo_root and os.path.isdir(temp_repo_root):
                shutil.rmtree(temp_repo_root, ignore_errors=True)

    # ── query handler ─────────────────────────────────────────────────────
    elif args.command == "query":
        target_path = os.path.abspath(args.repo_path)

        # .cartography/ is written to cwd (project root), not inside the repo
        # Keep cwd as-is; just point Navigator at the right artifacts
        cartography_dir = os.path.join(os.getcwd(), ".cartography")

        # Lazy import so LangGraph deps aren't required for analyze-only usage
        try:
            from .agents.navigator import Navigator
        except ImportError as e:
            logger.error("Navigator dependencies missing: %s", e)
            logger.info("Run: uv pip install langgraph langchain-core langchain-ollama")
            sys.exit(1)

        if not os.path.exists(os.path.join(cartography_dir, "lineage_graph.json")):
            logger.error("No .cartography/ artifacts found.")
            logger.info("Run 'analyze' first: uv run python -m src.cli analyze %s", args.repo_path)
            sys.exit(1)

        navigator = Navigator()

        if args.ask:
            # Single-shot mode
            print(f"\n[Navigator] Query: {args.ask}\n")
            try:
                answer = navigator.query(args.ask)
                print(answer)
            except Exception as e:
                logger.error("Navigator Query Error: %s", e)
                sys.exit(1)
        else:
            # Interactive mode
            navigator.run_interactive()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
