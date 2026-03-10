import sys
import os
import argparse
from .orchestrator import Orchestrator

def main():
    """
    Main entry point for the Brownfield Cartographer CLI.
    """
    parser = argparse.ArgumentParser(
        description="Brownfield Cartographer: A tool for mapping modular dependencies and data lineage."
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # 'analyze' command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze a local repository")
    analyze_parser.add_argument(
        "repo_path", 
        help="Path to the repository to analyze (e.g., jaffle_shop)"
    )
    
    args = parser.parse_args()
    
    if args.command == "analyze":
        # Resolve the absolute path of the target repo
        target_path = os.path.abspath(args.repo_path)
        
        if not os.path.exists(target_path):
            print(f"[ERROR] Repository path does not exist: {target_path}")
            sys.exit(1)
            
        # Initialize and run the orchestrator
        orchestrator = Orchestrator(target_path)
        orchestrator.run_analysis()
        
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
