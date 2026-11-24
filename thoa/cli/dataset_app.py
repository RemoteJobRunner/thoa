import os
import typer
from thoa.core.api_utils import api_client
from thoa.core.dataset_utils import list_datasets, download_dataset, list_files_in_dataset
from thoa.core.input_specs import InputSpecError, parse_input_spec
from thoa.core.job_utils import collect_files
from thoa.core.reconnect import TRANSIENT_ERRORS, describe_error
from thoa.core.local_transfer import (
    ClientHeartbeat,
    prepare_mixed_transfer,
    start_transfer,
    track_transfer,
    upload_local_items,
)
from rich.panel import Panel
from rich.console import Console
from typing import List
from typing import Optional

console = Console()

app = typer.Typer(help="Dataset-related commands", context_settings={"help_option_names": ["-h", "--help"]})

@app.command("list")
def list_(
    n: int = typer.Option(None, "--number", "-n", help="Number of datasets to display. If not set, displays all."),
    sort_by: str = typer.Option("created", "--sort-by", "-s", help="Field to sort datasets by (e.g., created, size, n_files)."),
    descending: bool = typer.Option(True, "--desc", help="Sort in descending order.")
): 
    """List datasets"""

    if sort_by not in {"created", "files", "size"}:
        
        console.print(
            Panel(
                "[yellow]Can only sort by 'created', 'files', or 'size'.[/yellow]",
                title="[bold red]Error[/bold red]",
                style="red"
            )
        )

        raise typer.Exit(1)
    
    list_datasets(n, sort_by, not descending)
    
@app.command("create")
def create(
    inputs: List[str] = typer.Option(
        ..., "--input", "-i", help="Input to add to the dataset. Accepts everything 'thoa run --input' does: "
        "local paths, Google Drive URLs and public accessions, each optionally as <source>::<mount_path>. "
        "Repeat for several inputs.",
    ),
):
    """Create a dataset from inputs without running a job, to reuse with 'thoa run --input-dataset'."""
    try:
        specs = [parse_input_spec(raw) for raw in inputs]
    except InputSpecError as exc:
        console.print(f"[bold red]Error:[/bold red] {exc}")
        raise typer.Exit(code=1)

    unknown = [spec.raw for spec in specs if spec.kind == "unknown"]
    if unknown:
        console.print("[bold red]Error:[/bold red] Unsupported --input value(s): " + ", ".join(unknown))
        raise typer.Exit(code=1)

    # Catch empty local inputs before anything is sent to the server.
    empty = [spec.raw for spec in specs if spec.kind == "local" and not collect_files([spec.source])]
    if empty:
        console.print("[bold red]Error:[/bold red] No files found in: " + ", ".join(empty))
        raise typer.Exit(code=1)

    # Same steps 'thoa run' takes for its inputs, minus the job.
    prepared = prepare_mixed_transfer(specs, cwd=os.getcwd())
    if not prepared.items:
        console.print("[bold red]Error:[/bold red] The inputs contain no files.")
        raise typer.Exit(code=1)

    # The manifest has created the dataset: show its ID now, so an interrupted
    # import doesn't leave the user without it.
    transfer = api_client.get(f"/data-transfers/{prepared.transfer_id}") or {}
    dataset_id = transfer.get("dataset_public_id")
    if not dataset_id:
        console.print("[bold red]Error:[/bold red] The server did not return the dataset for this import.")
        raise typer.Exit(code=1)
    console.print(f"Dataset [cyan]{dataset_id}[/cyan] created, importing its files.")

    try:
        if prepared.has_local_uploads:
            # Only this CLI can upload them; the heartbeat lets the server tell if it goes away.
            with ClientHeartbeat(prepared.transfer_id):
                start_transfer(prepared)
                upload_local_items(prepared)
        else:
            start_transfer(prepared)
    except (KeyboardInterrupt, *TRANSIENT_ERRORS) as exc:
        reason = "Interrupted" if isinstance(exc, KeyboardInterrupt) else f"Lost connection ({describe_error(exc)})"
        console.print(
            f"\n[bold red]{reason} before all local files were uploaded.[/bold red] "
            f"Dataset {dataset_id} can't finish without them. Run the same command again: "
            "files already uploaded are skipped."
        )
        raise typer.Exit(code=130 if isinstance(exc, KeyboardInterrupt) else 1)

    try:
        track_transfer(prepared)
    except RuntimeError as exc:
        console.print(f"[bold red]{exc}[/bold red]")
        raise typer.Exit(code=1)
    except (KeyboardInterrupt, *TRANSIENT_ERRORS) as exc:
        # Every local file is uploaded, so the server finishes the import without us.
        reason = "Detached." if isinstance(exc, KeyboardInterrupt) else f"Lost connection ({describe_error(exc)})."
        console.print(
            f"\n[yellow]{reason} The import keeps running server-side.[/yellow]\n"
            f"  Dataset: [cyan]{dataset_id}[/cyan]\n"
            f"  Use it once the import finishes: [bold]thoa run --input-dataset {dataset_id} ...[/bold] "
            "(it tells you if the import is still running)"
        )
        raise typer.Exit(code=0 if isinstance(exc, KeyboardInterrupt) else 1)

    console.print(f"[bold green]Dataset created:[/bold green] [cyan]{dataset_id}[/cyan]")
    console.print(f"Use it with: [bold]thoa run --input-dataset {dataset_id} ...[/bold]")


@app.command("download")
def download(
    dataset_id: str = typer.Argument(..., help="The UUID of the dataset to download."),
    destination_path: str = typer.Argument(..., help="The path to download the dataset to."),
    include: List[str] = typer.Option(None, "--include", "-i", help="List of file public IDs to include. If not set, includes all files."),
    exclude: List[str] = typer.Option(None, "--exclude", "-e", help="List of file public IDs to exclude. If not set, excludes no files.")
):
    """Download a dataset by its UUID."""
    download_dataset(
        dataset_id, 
        destination_path,
        include=include,
        exclude=exclude
    )


@app.command("ls") 
def ls(
    dataset_id: str = typer.Argument(..., help="The UUID of the dataset to list files from."),
    level: Optional[int] = typer.Option(None, "--level", "-l", help="How many levels of the dataset hierarchy to display. If not set, all levels are displayed.")
):
    """List files in a dataset by its UUID."""
    
    list_files_in_dataset(dataset_id, level)

    
