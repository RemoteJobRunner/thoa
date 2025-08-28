import typer
from thoa.core.dataset_utils import list_datasets, download_dataset

app = typer.Typer(help="Dataset-related commands")

@app.command("list")
def list_(): 
    """List datasets"""
    list_datasets()
    
@app.command("download")
def download(
    dataset_id: str = typer.Argument(..., help="The UUID of the dataset to download."),
    destination_path: str = typer.Argument(..., help="The path to download the dataset to.")
):
    """Download a dataset by its UUID."""
    download_dataset(dataset_id, destination_path)

    
