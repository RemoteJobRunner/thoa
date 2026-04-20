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
        "disk_space": storage
    })

    return res is not None 


def collect_files(paths):
    all_files = []
    for path in paths:
        path = Path(os.path.abspath(path))
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


def file_sizes_in_bytes(paths, follow_symlinks=True):
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
    
    results = api_client.get(f"/jobs?public_id={job_id}")
    response = results[0] if results else {}
    
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

        metadata["upload"] = "complete"

        # Apply updated metadata
        blob_client.set_blob_metadata(metadata)

        # print(f"[SUCCESS] Uploaded {local_path.name} to {blob_client.blob_name}")
        print(f"[SUCCESS] Uploaded {local_path.name} to Thoa")
    except Exception as e:
        print(f"[ERROR] Failed to upload {local_path.name}: {e}")
        raise


def upload_all(upload_links, local_file_map, all_md5s, max_workers=4):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for link in upload_links:
            file_id = link["file_public_id"]
            local_path = Path(local_file_map.get(file_id))
            local_md5 = all_md5s.get(file_id)

            if not local_path.exists():
                print(f"[WARN] File missing: {file_id} -> {local_path}")
                continue

            # Skip upload if hash already matches
            if blob_exists_with_same_md5(link["url"], local_md5, local_path):
                print(f"[SKIP] {local_path.name} already uploaded with matching MD5")
                continue

            futures.append(executor.submit(upload_file_sas, local_path, link["url"], local_md5))

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception:
                pass 


def blob_exists_with_same_md5(sas_url: str, local_md5: str, local_path: Path | None = None) -> bool:
    # if we don't know local hash, we cannot prove anything -> do not skip
    if not local_md5:
        return False

    try:
        blob_client = BlobClient.from_blob_url(sas_url)
        props = blob_client.get_blob_properties()

        # optional but great: size guard
        if local_path is not None:
            local_size = local_path.stat().st_size
            remote_size = getattr(props, "size", None) or getattr(props, "content_length", None)
            if remote_size is None or int(remote_size) != int(local_size):
                return False

        md = props.metadata or {}
        remote_md5 = md.get("md5")
               
        if md.get("upload") != "complete":
            return False

        if remote_md5:
            return remote_md5 == local_md5

        # No usable checksum on the blob -> cannot prove match
        return False

    except Exception:
        return False
    

# Timestamp helpers
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


def _fmt_timestamp_detail(ts: str) -> str:
    dt = _parse_job_timestamp(ts)
    if not dt:
        return ts or "—"
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _fmt_duration(started_at: str, finished_at: str) -> str:
    start = _parse_job_timestamp(started_at)
    end = _parse_job_timestamp(finished_at)
    if not start or not end:
        return "—"
    total = int((end - start).total_seconds())
    mins, secs = divmod(total, 60)
    hours, mins = divmod(mins, 60)
    if hours:
        return f"{hours}h {mins}m {secs}s"
    if mins:
        return f"{mins} min {secs} sec"
    return f"{secs} sec"


def _fmt_size(bytes_val) -> str:
    if bytes_val is None:
        return "—"
    b = int(bytes_val)
    if b >= 1024 ** 3:
        return f"{b / (1024 ** 3):.1f} GB"
    if b >= 1024 ** 2:
        return f"{b / (1024 ** 2):.1f} MB"
    if b >= 1024:
        return f"{b / 1024:.1f} KB"
    return f"{b} B"


# Main job listing
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


def print_job_detail(job_id: str):
    with console.status("[bold cyan]Fetching job details...[/bold cyan]", spinner="dots12"):
        detail = api_client.get(f"/jobs/{job_id}/detail")
        logs = api_client.get(f"/jobs/{job_id}/log-entries", params={"limit": 500})

    if not detail:
        return

    name = detail.get("name") or str(detail.get("public_id", ""))[:8]
    console.print(f"\n[bold cyan]{name}[/bold cyan]\n")

    # Job Info
    started = str(detail.get("started_at") or "")
    finished = str(detail.get("finished_at") or "")

    info = Table(show_header=False, box=None, expand=False, padding=(0, 1))
    info.add_column(style="label", no_wrap=True)
    info.add_column(style="value")
    info.add_row("Name", name)
    info.add_row("Job ID", str(detail.get("public_id", "")))
    info.add_row("Started", _fmt_timestamp_detail(started))
    info.add_row("Finished", _fmt_timestamp_detail(finished))
    info.add_row("Duration", _fmt_duration(started, finished))
    info.add_row("CPU", str(detail.get("requested_cpu") or "—"))
    info.add_row("RAM", f"{detail.get('requested_ram') or '—'} GB")
    info.add_row("Disk", f"{detail.get('requested_disk_space') or '—'} GB")
    info.add_row("Credits used", str(detail.get("credits_debited") or "—"))
    info.add_row("Status", str(detail.get("status") or "—"))
    console.print(Panel(info, title="[bold]Job Info[/bold]", border_style="cyan"))

    # Input Dataset
    in_ds = detail.get("input_dataset")
    if in_ds:
        t = Table(show_header=False, box=None, expand=False, padding=(0, 1))
        t.add_column(style="label", no_wrap=True)
        t.add_column(style="value")
        t.add_row("ID", str(in_ds.get("public_id", ""))[:8])
        t.add_row("Created", _fmt_timestamp_detail(str(in_ds.get("created_at") or "")))
        t.add_row("Files", str(in_ds.get("number_of_files") or "—"))
        t.add_row("Size", _fmt_size(in_ds.get("total_size")))
        console.print(Panel(t, title="[bold]Input Dataset[/bold]", border_style="blue"))

    # Output Dataset
    out_ds = detail.get("output_dataset")
    if out_ds:
        t = Table(show_header=False, box=None, expand=False, padding=(0, 1))
        t.add_column(style="label", no_wrap=True)
        t.add_column(style="value")
        t.add_row("ID", str(out_ds.get("public_id", ""))[:8])
        t.add_row("Created", _fmt_timestamp_detail(str(out_ds.get("created_at") or "")))
        t.add_row("Files", str(out_ds.get("number_of_files") or "—"))
        t.add_row("Size", _fmt_size(out_ds.get("total_size")))
        console.print(Panel(t, title="[bold]Output Dataset[/bold]", border_style="blue"))

    # Environment
    env = detail.get("environment")
    if env:
        t = Table(show_header=False, box=None, expand=False, padding=(0, 1))
        t.add_column(style="label", no_wrap=True)
        t.add_column(style="value")
        env_id = str(env.get("public_id") or "")
        t.add_row("ID", (env_id[:8] + "…") if len(env_id) >= 8 else env_id)
        t.add_row("Type", str(env.get("env_type") or "—"))
        t.add_row("Status", str(env.get("env_status") or "—"))
        console.print(Panel(t, title="[bold]Environment[/bold]", border_style="green"))

    # Logs & Run Info
    run_cmd = detail.get("run_command") or "—"
    env_string = (env.get("env_string") or "—") if env else "—"

    if logs:
        from rich.markup import escape
        log_lines = [
            f"{_fmt_timestamp_detail(str(e.get('timestamp', '')))} \[{e.get('message_type', '')}] {escape(e.get('message') or '')}"
            for e in logs
        ]
        logs_text = "\n".join(log_lines)
    else:
        logs_text = "—"

    run_info = (
        f"[label]Run command:[/label]\n{run_cmd}\n\n"
        f"[label]Environment.yml:[/label]\n{env_string}\n\n"
        f"[label]Job Execution Logs:[/label]\n{logs_text}"
    )
    console.print(Panel(run_info, title="[bold]Logs & Run Info[/bold]", border_style="yellow"))
