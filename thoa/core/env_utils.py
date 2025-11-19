from typing import Optional
import os
import platform
import sys
import platform
import sys
from rich.console import Console
from rich.panel import Panel

console = Console()

def resolve_environment_spec(env_source: Optional[str]) -> str:
    """
    Resolve the environment specification from a given source.

    If the source is a path to a YAML file (e.g., environment.yml), this function reads and returns its contents as a string.

    Args:
        env_source (str): The source of the environment specification, such as a file path or environment name.

    Returns:
        str: The resolved environment specification.

    Raises:
        ValueError: If env_source is None or does not point to a valid .yml/.yaml file.
        FileNotFoundError: If the specified file does not exist.
        IOError: If the file cannot be read.
    """
    if env_source is None:
        return ""

    if not env_source.endswith((".yml", ".yaml")):
        raise ValueError(f"Unsupported environment source format: {env_source}")

    if not os.path.isfile(env_source):
        raise FileNotFoundError(f"Environment file not found: {env_source}")

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
