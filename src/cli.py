import sys
import os
import argparse
from .orchestrator import Orchestrator


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
        "repo_path",
        help="Path to the repository to analyze (local path or GitHub URL)"
    )
    analyze_parser.add_argument(
        "--no-semanticist",
        action="store_true",
        default=False,
        help="Skip the Semanticist agent (faster iteration, no LLM calls)"
    )

    # ── query ─────────────────────────────────────────────────────────────
    query_parser = subparsers.add_parser(
        "query", help="Query the knowledge graph interactively via the Navigator agent"
    )
    query_parser.add_argument(
        "repo_path",
        help="Path to the repository whose .cartography/ artifacts to query"
    )
    query_parser.add_argument(
        "--ask",
        metavar="QUESTION",
        default=None,
        help="Run a single query and exit (non-interactive). "
             "e.g. --ask 'What produces ventes_immobilieres?'"
    )

    args = parser.parse_args()

    # ── analyze handler ───────────────────────────────────────────────────
    if args.command == "analyze":
        target_path = os.path.abspath(args.repo_path)
        if not os.path.exists(target_path):
            print(f"[ERROR] Repository path does not exist: {target_path}")
            sys.exit(1)

        orchestrator = Orchestrator(
            target_path,
            skip_semanticist=args.no_semanticist,
        )
        orchestrator.run_analysis()

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
            print(f"[ERROR] Navigator dependencies missing: {e}")
            print("Run: uv pip install langgraph langchain-core langchain-ollama")
            sys.exit(1)

        if not os.path.exists(os.path.join(cartography_dir, "lineage_graph.json")):
            print(f"[ERROR] No .cartography/ artifacts found.")
            print(f"Run 'analyze' first: uv run python -m src.cli analyze {args.repo_path}")
            sys.exit(1)

        navigator = Navigator()

        if args.ask:
            # Single-shot mode
            print(f"\n[Navigator] Query: {args.ask}\n")
            try:
                answer = navigator.query(args.ask)
                print(answer)
            except Exception as e:
                print(f"[Navigator] Error: {e}")
                sys.exit(1)
        else:
            # Interactive mode
            navigator.run_interactive()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()