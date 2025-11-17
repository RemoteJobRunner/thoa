import typer
from typing import Optional, List
import pathlib
from thoa.core.api_utils import api_client

from rich.table import Table
from rich.panel import Panel
from rich.console import Console
from rich.theme import Theme
from rich import print as rprint
from rich.spinner import Spinner
from rich import box
from thoa.core import resolve_environment_spec
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from datetime import datetime

import concurrent.futures
from azure.storage.blob import BlobClient

import time
import hashlib
import mmap
from pathlib import Path
import os
max_threads = min(32, os.cpu_count() * 2) 

console = Console(theme=Theme({
    "label": "bold cyan",
    "value": "white",
    "title": "bold green",
    "warning": "bold red"
}))

def print_config(
    inputs,
    input_dataset,
    output,
    n_cores,
    ram,
    storage,
    tools,
    env_source,
    cmd,
    download_path,
    run_async,
    job_name,
    job_description,
    dry_run,
    verbose,
):
    config = {
        "Inputs": inputs,
        "Input Dataset": input_dataset,
        "Outputs": output,
        "Number of Cores": n_cores,
        "RAM": f"{ram} GB" if ram else None,
        "Storage": f"{storage} GB" if storage else None,
        "Tools": tools,
        "Environment Source": env_source,
        "Command": cmd,
        "Download Path": download_path,
        "Run Async": run_async,
        "Job Name": job_name,
        "Job Description": job_description,
        "Dry Run": dry_run,
        "Verbose": verbose,
    }

    table = Table(show_header=False, box=None, expand=False, padding=(0, 1))

    for key, val in config.items():
        table.add_row(f"[label]{key}[/label]", f"[value]{val}[/value]")

    panel = Panel(table, title="[title]Job Configuration[/title]", expand=False, border_style="green")
    console.print(panel)


def validate_user_command(
    n_cores: Optional[int] = None,
    ram: Optional[int] = None,
    storage: Optional[int] = None
): 
    """Validate whether the user has permission to start the requested run."""

    res = api_client.get("/users/validate_job_request", params={
        "n_cores": n_cores,
        "ram": ram,
        "disk_space": storage,
        "n_gpus": 0  # Assuming no GPUs for this example    
    })

    return res is not None 


def collect_files(paths):
    all_files = []
    for path in paths:
        if path.is_file():
            all_files.append(path)
        elif path.is_dir():
            all_files.extend(path.rglob("*"))  # recursive
    return [p for p in all_files if p.is_file()]  # filter out subdirs


def compute_md5_buffered(path):
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""): 
            h.update(chunk)
    return h.hexdigest()


def compute_md5_mmap(path):
    h = hashlib.md5()
    with path.open("rb") as f:
        with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            h.update(mm)
    return h.hexdigest()


def choose_hash_strategy(path, mmap_threshold_bytes=10 * 1024 * 1024):
    try:
        size = path.stat().st_size
        if size >= mmap_threshold_bytes:
            return path, compute_md5_mmap(path)
        else:
            return path, compute_md5_buffered(path)
    except Exception as e:
        return path, f"ERROR: {e}"
    

def hash_all(files, workers=max_threads):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        results = executor.map(choose_hash_strategy, files)
    return dict(results)


def file_sizes_in_bytes(paths, follow_symlinks=False):
    size_map = {}
    stack = [p for p in paths]

    while stack:
        path = stack.pop()

        try:
            if path.is_symlink() and not follow_symlinks:
                continue

            if path.is_file():
                size_map[path] = path.stat().st_size
            elif path.is_dir():
                with os.scandir(path) as entries:
                    for entry in entries:
                        try:
                            entry_path = Path(entry.path)
                            if entry.is_symlink() and not follow_symlinks:
                                continue
                            if entry.is_file(follow_symlinks=follow_symlinks):
                                size_map[entry_path] = entry.stat(follow_symlinks=follow_symlinks).st_size
                            elif entry.is_dir(follow_symlinks=follow_symlinks):
                                stack.append(entry_path)
                        except (FileNotFoundError, PermissionError):
                            pass  
        except (FileNotFoundError, PermissionError):
            pass

    return size_map


def current_job_status(job_id: str):
    """Fetch the current status of a job by its public ID."""
    
    response = api_client.get(f"/jobs?public_id={job_id}")[0] if api_client.get(f"/jobs?public_id={job_id}") else {}
    
    if response is None:
        raise ValueError(f"Job with ID {job_id} not found or invalid.")

    return response.get("status", "unknown")


def all_files_have_upload_links(job_id, input_dataset_id, file_public_ids):
    """Check if all files in the job have upload links."""
    
    links = api_client.get(
        f"/temporary_links", 
        params={
            "job_public_id": job_id,
            "dataset_public_id": input_dataset_id,
            "link_type": "upload"
        }
    )

    link_file_ids = {link['file_public_id'] for link in links}
    return set(file_public_ids).issubset(set(link_file_ids))


def upload_file_sas(local_path: Path, sas_url: str, local_md5: str, max_concurrency: int = 4):
    """
    Upload a file to Azure Blob Storage using a pre-signed SAS URL.
    Uses parallel uploads for large files.
    """

    try:
        blob_client = BlobClient.from_blob_url(sas_url)

        print(f"Uploading: {local_path} -> {blob_client.blob_name}")
        with open(local_path, "rb") as data:
            blob_client.upload_blob(
                data,
                overwrite=True,
                max_concurrency=max_concurrency,
                metadata={
                    "md5": local_md5,
                    "upload": "incomplete"
                },
                validate_content=True
            )

        # Retrieve existing metadata
        props = blob_client.get_blob_properties()
        metadata = props.metadata or {}

        # Update "upload" status
        metadata["upload"] = "complete"

        # Apply updated metadata
        blob_client.set_blob_metadata(metadata)

        print(f"[SUCCESS] Uploaded {local_path.name} to {blob_client.blob_name}")
    except Exception as e:
        print(f"[ERROR] Failed to upload {local_path.name}: {e}")
        raise


def upload_all(upload_links, local_file_map, all_md5s, max_workers=4):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for link in upload_links:
            file_id = link["file_public_id"]
            local_path = Path(local_file_map.get(file_id))
            local_md5 = all_md5s.get(local_path)

            if not local_path.exists():
                print(f"[WARN] File missing: {file_id} -> {local_path}")
                continue

            # Skip upload if hash already matches
            if blob_exists_with_same_md5(link["url"], local_md5):
                print(f"[SKIP] {local_path.name} already uploaded with matching MD5")
                continue

            futures.append(executor.submit(upload_file_sas, local_path, link["url"], local_md5))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                pass 


def blob_exists_with_same_md5(sas_url: str, local_md5: str) -> bool:
    try:
        blob_client = BlobClient.from_blob_url(sas_url)
        props = blob_client.get_blob_properties()
        remote_md5 = props.metadata.get("md5")

        return remote_md5 == local_md5
    except Exception as e:
        # Blob doesn't exist or cannot read metadata
        return False


# ---------------------------
# Timestamp helpers
# ---------------------------
def _parse_job_timestamp(ts: str):
    """Return a datetime object parsed from an ISO timestamp."""
    if not ts:
        return None
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
            pass
    return None


def _fmt_job_timestamp(ts: str) -> str:
    """Convert ISO timestamp to 'Mon DD YYYY, HH:MM' text."""
    dt = _parse_job_timestamp(ts)
    if not dt:
        return ts
    return dt.strftime("%b %d %Y, %H:%M")


# ---------------------------
# Main job listing
# ---------------------------
def list_jobs(
    limit: int = None,
    sort_by: str = "started",
    ascending: bool = False,
):
    try:
        with console.status("[bold cyan]Fetching jobs...[/bold cyan]", spinner="dots12"):
            jobs = api_client.get("/jobs")

        if not jobs:
            console.print(Panel("[yellow]No jobs found.[/yellow]", title="Jobs", style="bold"))
            return

        # Sorting
        def sort_key(job):
            if sort_by == "status":
                return job.get("status", "")
            return _parse_job_timestamp(job.get("started_at", "")) or datetime.min

        jobs = sorted(jobs, key=sort_key, reverse=not ascending)

        if limit:
            jobs = jobs[:limit]

        # Build table
        table = Table(title="THOA Jobs", box=box.MINIMAL_DOUBLE_HEAD)
        table.add_column("Name", style="cyan")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Started", style="green")
        table.add_column("Status", style="magenta")
        table.add_column("Input Dataset", style="blue")
        table.add_column("Output Dataset", style="blue")

        for job in jobs:
            input_ds = job.get("input_dataset_public_id")[:8] if job.get("input_dataset_public_id") else ""
            output_ds = job.get("output_dataset_public_id")[:8] if job.get("output_dataset_public_id") else ""

            table.add_row(
                job.get("name", ""),
                job.get("public_id", ""),
                _fmt_job_timestamp(job.get("started_at", "")),
                job.get("status", ""),
                input_ds,
                output_ds,
            )

        console.print(table)
        console.print(
            Panel(
                f"[green]Displayed {len(jobs)} job(s)[/green] "
                f"(sorted by [bold]{sort_by}[/bold]).",
                title="Summary",
                style="bold",
            )
        )

    except Exception as e:
        console.print(Panel(f"[red]Error listing jobs:[/red] {e}", title="Error", style="bold red"))
