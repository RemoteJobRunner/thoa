import typer
import yaml
from thoa.core.api_utils import api_client
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich import box
from datetime import datetime

console = Console()

app = typer.Typer(help="Environment-related commands")


def _fmt_ts(ts: str) -> str:
    if not ts:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt).strftime("%b %d %Y, %H:%M")
        except ValueError:
            pass
    return ts


def _status_text(status: str) -> Text:
    """Return a Rich Text object with colour-coded environment status."""
    colours = {
        "validated":        "bold green",
        "validation_failed":"bold red",
        "validating":       "bold blue",
        "created":          "bold yellow",
    }
    style = colours.get(status, "white")
    return Text(status, style=style)


def _extract_tools(env_string: str, limit: int = 3) -> str:
    """Parse conda env YAML and return the first `limit` deps, e.g. 'bwa, samtools=1.9 +2 more'."""
    if not env_string:
        return ""
    try:
        spec = yaml.safe_load(env_string)
        deps = spec.get("dependencies", [])
        # Keep only plain string entries (skip pip: {...} dicts)
        tools = [d for d in deps if isinstance(d, str)]
        if not tools:
            return ""
        shown = tools[:limit]
        remainder = len(tools) - limit
        result = ", ".join(shown)
        if remainder > 0:
            result += f" +{remainder} more"
        return result
    except Exception:
        return ""


@app.command("list")
def list_(
    n: int = typer.Option(None, "--number", "-n", help="Number of environments to display."),
):
    """List your environments."""
    try:
        with console.status("[bold cyan]Fetching environments...[/bold cyan]", spinner="dots12"):
            envs = api_client.get("/environments")

        if not envs:
            console.print(Panel("[yellow]No environments found.[/yellow]", title="Environments"))
            return

        envs = sorted(envs, key=lambda e: e.get("created_at", ""), reverse=True)

        if n:
            envs = envs[:n]

        table = Table(title="THOA Environments", box=box.MINIMAL_DOUBLE_HEAD)
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Type", style="magenta")
        table.add_column("Status", no_wrap=True)
        table.add_column("Created", style="blue")
        table.add_column("Tools")

        for env in envs:
            table.add_row(
                str(env.get("public_id", "")),
                env.get("env_type", ""),
                _status_text(env.get("env_status", "")),
                _fmt_ts(env.get("created_at", "")),
                _extract_tools(env.get("env_string", "")),
            )

        console.print(table)
        console.print(Panel(
            f"[green]Displayed {len(envs)} environment(s)[/green]",
            title="Summary",
            style="bold",
        ))

    except Exception as e:
        console.print(Panel(f"[red]Error listing environments:[/red] {e}", title="Error", style="bold red"))


@app.command("show")
def show(
    env_uuid: str = typer.Argument(..., help="UUID of the environment to display."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Also show build logs."),
):
    """Show full details of an environment."""
    try:
        with console.status("[bold cyan]Fetching environment...[/bold cyan]", spinner="dots12"):
            results = api_client.get(f"/environments?public_id={env_uuid}")

        if not results:
            console.print(f"[bold red]Error:[/bold red] No environment found with ID [cyan]{env_uuid}[/cyan].")
            raise typer.Exit(1)

        env = results[0] if isinstance(results, list) else results

        # ── Header info ──────────────────────────────────────────────
        info = Table(show_header=False, box=None, padding=(0, 1))
        info.add_column("Field", style="bold cyan")
        info.add_column("Value", style="white")

        info.add_row("ID",       str(env.get("public_id", "")))
        info.add_row("Type",     env.get("env_type", ""))
        info.add_row("Status",   _status_text(env.get("env_status", "")))
        info.add_row("Security", env.get("security_status", ""))
        info.add_row("Created",  _fmt_ts(env.get("created_at", "")))
        info.add_row("Updated",  _fmt_ts(env.get("updated_at", "")))

        console.print(Panel(info, title="[bold green]Environment[/bold green]", expand=False))

        # ── Environment string (full YAML) ────────────────────────────
        env_string = env.get("env_string", "")
        if env_string:
            console.print(Panel(
                env_string.strip(),
                title="[bold cyan]Environment Spec (YAML)[/bold cyan]",
                expand=False,
            ))
        else:
            console.print("[dim]No environment spec available.[/dim]")

        # ── Build logs (only with -v) ─────────────────────────────────
        if verbose:
            build_logs = env.get("build_logs", "")
            if build_logs:
                console.print(Panel(
                    build_logs.strip(),
                    title="[bold yellow]Build Logs[/bold yellow]",
                    expand=False,
                ))
            else:
                console.print("[dim]No build logs available.[/dim]")

    except typer.Exit:
        raise
    except Exception as e:
        console.print(Panel(f"[red]Error fetching environment:[/red] {e}", title="Error", style="bold red"))
