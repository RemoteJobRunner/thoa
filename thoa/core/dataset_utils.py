from uuid import UUID
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.spinner import Spinner
from rich import box
from .api_utils import api_client as client
from pathlib import Path

console = Console()

def _format_timestamp(ts: str) -> str:
    """Convert ISO timestamp to 'Mon DD YYYY, HH:MM' format."""
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # microseconds + Z
        "%Y-%m-%dT%H:%M:%SZ",     # no microseconds + Z
        "%Y-%m-%dT%H:%M:%S.%f",   # microseconds, no Z
        "%Y-%m-%dT%H:%M:%S",      # no microseconds, no Z
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.strftime("%b %d %Y, %H:%M")
        except ValueError:
            continue
    return ts  # fallback if no format matched

def _parse_timestamp(ts: str):
    """Parse ISO-like timestamp to datetime; return None if unparseable."""
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt)
        except ValueError:
            continue
    return None

def list_datasets(n: int = None, sort_by: str = "created", ascending: bool = True):
    """
    List datasets with Rich output.

    Args:
        n (int, optional): Limit number of datasets displayed (after sorting).
        sort_by (str): Field to sort by: 'created', 'files', or 'size'.
        ascending (bool): Sort order; True=ascending, False=descending.
    """
    try:
        with console.status("[bold cyan]Fetching datasets...[/bold cyan]", spinner="dots12"):
            my_datasets = client.get("/datasets")

        if not my_datasets:
            console.print(Panel("[yellow]No datasets found.[/yellow]", title="Datasets", style="bold"))
            return

        # Normalize and validate sort_by
        sort_key = (sort_by or "created").strip().lower()
        if sort_key not in {"created", "files", "size"}:
            console.print(Panel(
                f"[yellow]Unknown sort_by='{sort_by}'. Using 'created'.[/yellow]",
                title="Notice",
                style="bold"
            ))
            sort_key = "created"

        # Build key function
        def key_func(d):
            if sort_key == "files":
                return d.get("number_of_files", 0)
            if sort_key == "size":
                # total_size expected in bytes; keep numeric for stable sort
                return d.get("total_size", 0)
            # created
            dt = _parse_timestamp(d.get("created_at", ""))
            return dt or datetime.min  # put unparsable at start/end depending on order

        # Sort then slice
        my_datasets = sorted(my_datasets, key=key_func, reverse=not ascending)
        if n is not None and n > 0:
            my_datasets = my_datasets[:n]

        # Render table
        table = Table(title="Available Datasets", box=box.MINIMAL_DOUBLE_HEAD)
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Created", style="green")
        table.add_column("Files", style="magenta", justify="right")
        table.add_column("Size (GB)", style="blue", justify="right")

        for dataset in my_datasets:
            size_gb = round(dataset.get("total_size", 0) / 1024 ** 3, 2)
            table.add_row(
                dataset.get("public_id", ""),
                _format_timestamp(dataset.get("created_at", "")),
                str(dataset.get("number_of_files", 0)),
                f"{size_gb}"
            )

        console.print(table)
        order = "ascending" if ascending else "descending"
        console.print(Panel(
            f"[green]Displayed {len(my_datasets)} dataset(s)[/green] "
            f"(sorted by [bold]{sort_key}[/bold], {order}).",
            title="Summary",
            style="bold"
        ))

    except Exception as e:
        console.print(Panel(f"[red]Error listing datasets:[/red] {e}", title="Error", style="bold red"))

def download_dataset(dataset_id: UUID, destination_path: str):
    """Download all files in the dataset to the specified destination path."""
    with console.status(f"[bold green]Preparing to download dataset [/bold green][bold cyan]{dataset_id}[/bold cyan][bold green] ...[/bold green]", spinner="dots12"):

        try: 
            dataset = client.get(f"/datasets?public_id={dataset_id}")
            
            if not dataset:
                console.print(Panel(f"[red]Dataset {dataset_id} not found.[/red]", title="Error", style="bold red"))
                return
            
            dataset = dataset[0]
            dgb = round(dataset.get("total_size", 0) / 1024**3, 2)

            # First get the largest common path prefix
            files = dataset.get("context", {})
            all_paths = list(files.keys())
            if not all_paths:
                console.print(Panel(f"[yellow]Dataset {dataset_id} has no files to download.[/yellow]", title="Notice", style="bold"))
                return

            common_prefix = all_paths[0]
            for p in all_paths[1:]:
                while not p.startswith(common_prefix):
                    common_prefix = common_prefix[:-1]
                    if not common_prefix:
                        break

            if common_prefix == all_paths[0]:
                common_prefix = "/"

            # Create download links for each file
            download_links = {}

            for path_string, file_id in files.items():

                if str(file_id) in download_links:
                    continue  # Skip if already have link for this file ID
                
                requested_link = client.post(f"/temporary_links/{file_id}/request-download")
                download_links[str(file_id)] = requested_link

            download_path = Path(destination_path).resolve()
            download_path.mkdir(parents=True, exist_ok=True)

        except Exception as e:
            console.print(Panel(f"[red]Error fetching dataset {dataset_id}:[/red] {e}", title="Error", style="bold red"))
            return
        
    with console.status(f"[bold green]Downloading {dataset.get('number_of_files', 0)} files for dataset [/bold green][bold cyan]{dataset_id}[/bold cyan][bold green] to {destination_path} ({dgb} total Gb).[/bold green]", spinner="dots12"):
        try:
            
            for path_string, file_id in files.items():
                link_info = download_links.get(str(file_id))
                if not link_info:
                    console.print(Panel(f"[red]No download link for file ID {file_id}. Skipping.[/red]", title="Warning", style="bold red"))
                    continue
                
                url = link_info.get("url")
                download_path_without_common_prefix = path_string.lstrip(common_prefix.lstrip("/"))
                download_location = str(download_path) +  "/" +  str(download_path_without_common_prefix)
                # download_location.parent.mkdir(parents=True, exist_ok=True)

                # download_location = download_path / Path(path_string).relative_to(common_prefix.lstrip("/"))
                print(f"Downloading {path_string} to {download_location} ...")
                # download_location.parent.mkdir(parents=True, exist_ok=True)
        
        finally:
            pass