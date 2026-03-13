import os
import subprocess


def get_git_change_velocity(repo_path: str, file_rel_path: str) -> int:
    """
    Computes the number of commits that have touched a specific file.
    This represents the 'change velocity' of the module.
    """
    try:
        # Run git rev-list --count HEAD <file> to get the number of commits
        result = subprocess.run(
            ["git", "rev-list", "--count", "HEAD", file_rel_path],
            cwd=repo_path,
            capture_output=True,
            text=True,
            check=True,
        )
        return int(result.stdout.strip())
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
            rel_path = os.path.relpath(full_path, repo_path)
            velocities[rel_path] = get_git_change_velocity(repo_path, rel_path)
    return velocities
