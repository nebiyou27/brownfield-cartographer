import os
import subprocess

from ..path_utils import normalize_path_key, with_path_aliases


def get_git_change_velocity(repo_path: str, file_rel_path: str, days: int = 90) -> int:
    """
    Computes the number of commits that have touched a specific file.
    This represents the 'change velocity' of the module.
    """
    try:
        normalized_rel_path = normalize_path_key(file_rel_path)
        # Count commits touching the file within the configured recent window.
        result = subprocess.run(
            [
                "git",
                "log",
                f"--since={days}.days",
                "--pretty=format:%H",
                "--",
                normalized_rel_path,
            ],
            cwd=repo_path,
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
