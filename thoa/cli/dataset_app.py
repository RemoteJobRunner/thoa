import typer
from thoa.core.dataset_utils import list_datasets, download_dataset
from rich.panel import Panel
from rich.console import Console

console = Console()

app = typer.Typer(help="Dataset-related commands")

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

        raise typer.Exit()
    
    list_datasets(n, sort_by, not descending)
    
@app.command("download")
def download(
    dataset_id: str = typer.Argument(..., help="The UUID of the dataset to download."),
    destination_path: str = typer.Argument(..., help="The path to download the dataset to.")
):
    """Download a dataset by its UUID."""
    download_dataset(dataset_id, destination_path)

    
