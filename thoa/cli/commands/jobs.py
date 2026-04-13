import typer
import time
from thoa.core.job_utils import list_jobs, current_job_status
from thoa.core.api_utils import api_client
from rich.console import Console
from rich.panel import Panel

console = Console()

app = typer.Typer(help="Job-related commands")

TERMINAL_STATUSES = {
    "completed", "failed_execution", "failed_validation",
    "failed_provisioning", "failed_upload", "failed_startup", "cancelled"
}


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


@app.command("attach")
def attach(
    job_id: str = typer.Argument(..., help="Public ID of the job to attach to."),
):
    """Attach to a running job and stream its logs."""
    status = current_job_status(job_id)

    if status in TERMINAL_STATUSES:
        console.print(f"Job [cyan]{job_id}[/cyan] already [bold]{status}[/bold].")
        return

    if status != "running":
        with console.status(f"Waiting for job to start running (current: {status})", spinner="dots12"):
            while status not in TERMINAL_STATUSES and status != "running":
                time.sleep(4)
                status = current_job_status(job_id)

        if status in TERMINAL_STATUSES:
            console.print(f"Job [cyan]{job_id}[/cyan] ended with status [bold]{status}[/bold].")
            return

    api_client.stream_logs_blocking(job_id, from_id="0-0")
