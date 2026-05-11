from typing import Optional
import os
import platform
import subprocess
import sys
import yaml
from rich.console import Console
from rich.panel import Panel

console = Console()


def _pip_lines_to_conda_yaml(lines: list[str]) -> str:
    """Convert a list of requirements-format lines to a conda environment YAML string."""
    pip_deps = []
    for line in lines:
        line = line.strip()
        # skip blanks, comments, and pip options (e.g. -r, --index-url)
        if not line or line.startswith("#") or line.startswith("-"):
            continue
        pip_deps.append(line)

    if not pip_deps:
        raise ValueError("No packages found in requirements.")

    spec = {
        "name": "env",
        "channels": ["conda-forge", "bioconda", "defaults"],
        "dependencies": ["pip", {"pip": pip_deps}],
    }
    return yaml.dump(spec, sort_keys=False, default_flow_style=False)


def _requirements_txt_to_conda_yaml(path: str) -> str:
    """Convert a requirements.txt file to a conda environment YAML string."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as e:
        raise IOError(f"Failed to read requirements file: {e}")

    try:
        return _pip_lines_to_conda_yaml(lines)
    except ValueError:
        raise ValueError(f"No packages found in requirements file: {path}")


def _capture_current_environment() -> str:
    """
    Capture the active Python environment as a conda YAML string.

    Priority:
      1. Non-base conda env  → `conda env export --no-builds`
      2. Everything else     → `pip freeze` on the current Python interpreter
    """
    conda_env = os.environ.get("CONDA_DEFAULT_ENV")
    conda_prefix = os.environ.get("CONDA_PREFIX")

    if conda_prefix and conda_env and conda_env != "base":
        try:
            result = subprocess.run(
                ["conda", "env", "export", "--no-builds"],
                capture_output=True, text=True, check=True,
            )
            return result.stdout
        except FileNotFoundError:
            raise RuntimeError("conda not found in PATH; cannot export environment.")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"conda env export failed: {e.stderr.strip()}")

    # Venv, conda base, or bare system Python — all handled via pip freeze
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "freeze"],
            capture_output=True, text=True, check=True,
        )
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"pip freeze failed: {e.stderr.strip()}")

    if not result.stdout.strip():
        raise ValueError("Current environment has no installed packages (pip freeze returned empty).")

    pip_yaml = _pip_lines_to_conda_yaml(result.stdout.splitlines())

    # Inject the exact Python version so conda picks the right interpreter
    import yaml as _yaml
    spec = _yaml.safe_load(pip_yaml)
    py = f"python>={sys.version_info.major}.{sys.version_info.minor}"
    deps = spec.get("dependencies", [])
    if not any(isinstance(d, str) and d.startswith("python") for d in deps):
        deps.insert(0, py)
    spec["dependencies"] = deps
    return _yaml.dump(spec, sort_keys=False, default_flow_style=False)


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

    if env_source == "use-current":
        return _capture_current_environment()

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
