import os
import subprocess
from pathlib import Path

from ..path_utils import normalize_path_key, with_path_aliases


def _resolve_git_root(repo_path: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "-C", repo_path, "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )
        git_root = result.stdout.strip()
        return git_root if git_root else None
    except Exception:
        return None


def get_git_change_velocity(repo_path: str, file_rel_path: str, days: int = 90) -> int:
    """
    Computes the number of commits that have touched a specific file.
    This represents the 'change velocity' of the module.
    """
    try:
        normalized_rel_path = normalize_path_key(file_rel_path)
        git_root = _resolve_git_root(repo_path)
        if not git_root:
            return 0

        repo_abs = Path(repo_path).resolve()
        git_root_abs = Path(git_root).resolve()
        target_abs = (repo_abs / normalized_rel_path).resolve()
        try:
            path_for_git = normalize_path_key(str(target_abs.relative_to(git_root_abs)))
        except ValueError:
            # File path is outside the git repository root.
            return 0

        # Count commits touching the file within the configured recent window.
        result = subprocess.run(
            [
                "git",
                "-C",
                str(git_root_abs),
                "log",
                f"--since={days}.days",
                "--pretty=format:%H",
                "--",
                path_for_git,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        output = result.stdout.strip()
        if not output:
            return 0
        return len([line for line in output.splitlines() if line.strip()])
    except Exception:
        # If not a git repo or file not found, return 0
        return 0


def get_all_file_velocities(repo_path: str) -> dict:
    """
    Returns a mapping of relative file paths to their commit counts.
    """
    velocities = {}
    for root, dirs, files in os.walk(repo_path):
        if ".git" in dirs:
            dirs.remove(".git")
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = normalize_path_key(os.path.relpath(full_path, repo_path))
            velocity = get_git_change_velocity(repo_path, rel_path)
            with_path_aliases(velocities, rel_path, velocity)
    return velocities
