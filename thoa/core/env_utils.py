from typing import Optional
import os
import platform
import sys
from rich.console import Console
from rich.panel import Panel

console = Console()

def _requirements_txt_to_conda_yaml(path: str) -> str:
    """Convert a requirements.txt file to a conda environment YAML string."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        raise IOError(f"Failed to read requirements file: {e}")

    pip_deps = []
    for line in lines:
        line = line.strip()
        # skip blanks, comments, and pip options (e.g. -r, --index-url)
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        pip_deps.append(line)

    if not pip_deps:
        raise ValueError(f"No packages found in requirements file: {path}")

    import yaml
    spec = {
        "name": "env",
        "channels": ["conda-forge", "bioconda", "defaults"],
        "dependencies": ["pip", {"pip": pip_deps}],
    }
    return yaml.dump(spec, sort_keys=False, default_flow_style=False)


def resolve_environment_spec(env_source: Optional[str]) -> str:
    """
    Resolve the environment specification from a given source.

    Accepts a conda environment YAML (.yml/.yaml) or a pip requirements
    file (.txt), which is converted to a conda environment YAML string.

    Args:
        env_source (str): Path to an environment.yml or requirements.txt file.

    Returns:
        str: The resolved environment specification as a conda YAML string.

    Raises:
        ValueError: If env_source is None or the file format is unsupported.
        FileNotFoundError: If the specified file does not exist.
        IOError: If the file cannot be read.
    """
    if env_source is None:
        return ""

    env_source = str(env_source)

    if not env_source.endswith((".yml", ".yaml", ".txt")):
        raise ValueError(f"Unsupported environment source format: {env_source}. Expected .yml, .yaml, or .txt (requirements).")

    if not os.path.isfile(env_source):
        raise FileNotFoundError(f"Environment file not found: {env_source}")

    if env_source.endswith(".txt"):
        return _requirements_txt_to_conda_yaml(env_source)

    try:
        with open(env_source, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        raise IOError(f"Failed to read environment file: {e}")

def is_wsl() -> bool:
    """Return True if running under Windows Subsystem for Linux."""
    try:
        return "microsoft" in platform.release().lower()
    except Exception:
        return False


def block_windows_unless_wsl() -> None:
    """
    Block execution on native Windows (PowerShell, CMD, Git Bash).
    Allow Linux, macOS, and Windows Subsystem for Linux (WSL).
    """
    system = platform.system().lower()

    # Native Windows → block (only allow WSL)
    if system == "windows" and not is_wsl():
        console.print(
            Panel(
                "[red]This tool does not support running directly on Windows.[/red]\n\n"
                "To use it on a Windows machine, please install and run it via:\n"
                "[bold cyan]Windows Subsystem for Linux (WSL)[/bold cyan]\n\n"
                "Official installation guide:\n"
                "[blue]https://learn.microsoft.com/en-us/windows/wsl/install[/blue]\n\n"
                "Supported environments:\n"
                "• [green]Linux[/green]\n"
                "• [green]macOS[/green]\n"
                "• [green]Windows Subsystem for Linux (WSL)[/green]",
                title="Unsupported Environment",
                style="bold red",
            )
        )
        sys.exit(1)
