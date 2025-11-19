import typer
from thoa.core.job_utils import list_jobs
from rich.console import Console
from rich.panel import Panel

console = Console()

app = typer.Typer(help="Job-related commands")


@app.command("list")
def list_(
    n: int = typer.Option(None, "--number", "-n", help="Number of jobs to display."),
    sort_by: str = typer.Option("started", "--sort-by", "-s", help="Sort by: started or status"),
    ascending: bool = typer.Option(False, "--asc", help="Sort ascending (default is descending)."),
):
    """List recent jobs."""

    if sort_by not in {"started", "status"}:
        console.print(Panel("[yellow]sort_by must be 'started' or 'status'[/yellow]", title="Error"))
        raise typer.Exit(1)

    list_jobs(
        limit=n,
        sort_by=sort_by,
        ascending=ascending,
    )
