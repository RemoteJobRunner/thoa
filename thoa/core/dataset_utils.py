from uuid import UUID
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.spinner import Spinner
from rich import box
from .api_utils import api_client as client
from pathlib import Path
from azure.storage.blob import BlobClient
from thoa.core.job_utils import compute_md5_buffered
import os
import shutil

from uuid import UUID
from datetime import datetime
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.spinner import Spinner
from rich import box
from .api_utils import api_client as client
from pathlib import Path, PurePath
from azure.storage.blob import BlobClient
from thoa.core.job_utils import compute_md5_buffered
import os
import shutil
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
import fnmatch

console = Console()

FILE_WORKERS = min(16, (os.cpu_count() or 4) * 2)
PER_BLOB_CONCURRENCY = 8                           
CHUNK_SIZE = 8 * 1024 * 1024                       
VERIFY_MD5 = False                                 

def _filter_files_by_id_or_path(
    files: dict[str, str],
    include: list[str] | None,
    exclude: list[str] | None,
) -> dict[str, str]:
    """
    Filter files dict (path -> file_id) by include/exclude rules.
    Rules may be path globs (on path strings) or exact file IDs.
    """
    if not include and not exclude:
        return files

    include = [str(x) for x in (include or [])]
    exclude = [str(x) for x in (exclude or [])]

    result = {}
    for path, fid in files.items():
        fid_str = str(fid)

        # check include
        if include:
            ok = any(
                fnmatch.fnmatch(path, pat) or fid_str == pat
                for pat in include
            )
            if not ok:
                continue

        # check exclude
        skip = any(
            fnmatch.fnmatch(path, pat) or fid_str == pat
            for pat in exclude
        )
        if skip:
            continue

        result[path] = fid
    return result


def _fmt_bytes(n: int) -> str:
    for unit in ["B","KiB","MiB","GiB","TiB","PiB"]:
        if n < 1024 or unit == "PiB":
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} B"

def _nearest_existing_parent(path: Path) -> Path:
    p = path
    while not p.exists():
        if p.parent == p:
            break
        p = p.parent
    return p

def _available_bytes(path: Path) -> int:
    base = _nearest_existing_parent(path)
    try:
        st = os.statvfs(str(base)) 
        return st.f_bavail * st.f_frsize
    except AttributeError:
        total, used, free = shutil.disk_usage(str(base)) 
        return free

def _required_with_headroom(total_size_bytes: int) -> int:
    headroom = max(int(total_size_bytes * 0.05), 200 * 1024 * 1024)
    return total_size_bytes + headroom


def _format_timestamp(ts: str) -> str:
    """Convert ISO timestamp to 'Mon DD YYYY, HH:MM' format."""
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  
        "%Y-%m-%dT%H:%M:%SZ",     
        "%Y-%m-%dT%H:%M:%S.%f",   
        "%Y-%m-%dT%H:%M:%S",      
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(ts, fmt)
            return dt.strftime("%b %d %Y, %H:%M")
        except ValueError:
            continue
    return ts  

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

def _ensure_parent(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)

def _sizes_match(local: Path, expected_size: int) -> bool:
    try:
        return local.exists() and local.stat().st_size == expected_size
    except Exception:
        return False

def _normalize_md5_hex_or_b64_to_hex(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    if len(s) == 32 and all(c in "0123456789abcdefABCDEF" for c in s):
        return s.lower()
    try:
        b = base64.b64decode(s)
        if len(b) == 16:
            return b.hex()
    except Exception:
        pass
    return None

def _get_size_and_remote_md5(blob: BlobClient) -> tuple[int | None, str | None]:
    """Use EXACTLY your source of truth: metadata['md5'] (hex or base64)."""
    try:
        props = blob.get_blob_properties()
        size = getattr(props, "size", None)
        meta = getattr(props, "metadata", None) or {}
        md5_hex = _normalize_md5_hex_or_b64_to_hex(meta.get("md5"))
        return size, md5_hex
    except Exception:
        return None, None

def _extract_url(link_info: dict | None) -> str | None:
    """Handle common server payload shapes."""
    if not link_info:
        return None
    return link_info.get("url") or link_info.get("sas_url") or None

def _safe_dest(base_dir: Path, key_path: str) -> Path:
    """
    Join 'key_path' under base_dir, even if key is absolute.
    If absolute, drop the root and keep the tail so we never escape base_dir.
    """
    p = Path(key_path)
    if p.is_absolute():
        tail = Path(*p.parts[1:]) if len(p.parts) > 1 else Path(p.name)
        return (base_dir / tail).resolve()
    return (base_dir / p).resolve()


# Per-file worker
def _download_one(path_string: str,
                  file_id: str,
                  link_info: dict,
                  base_dir: Path,
                  verify_md5: bool,
                  per_blob_concurrency: int,
                  chunk_size: int) -> tuple[str, str, bool, str]:
    """
    Returns (path_string, file_id, ok, note)
    note in {"skipped_exists_verified","skipped_exists","downloaded_verified","downloaded","no_url","md5_mismatch","error:..."}
    """
    sas_url = _extract_url(link_info)
    if not sas_url:
        return (path_string, file_id, False, "no_url")

    dest = _safe_dest(base_dir, path_string)
    _ensure_parent(dest)
    tmp = dest.with_suffix(dest.suffix + ".part")

    blob = BlobClient.from_blob_url(sas_url)

    expected_size, remote_md5_hex = _get_size_and_remote_md5(blob)

    if expected_size is not None and _sizes_match(dest, expected_size):
        if verify_md5 and remote_md5_hex:
            try:
                local_md5 = (compute_md5_buffered(dest) or "").lower()
                if local_md5 == remote_md5_hex:
                    return (path_string, file_id, True, "skipped_exists_verified")
                dest.unlink(missing_ok=True)
            except Exception:
                pass
        else:
            return (path_string, file_id, True, "skipped_exists")

    try:
        downloader = blob.download_blob(max_concurrency=per_blob_concurrency)

        with open(tmp, "wb") as fh:
            try:
                downloader.readinto(fh)
            except TypeError:
                try:
                    for chunk in downloader.chunks(chunk_size=chunk_size):
                        fh.write(chunk)
                except TypeError:
                    for chunk in downloader.chunks():
                        fh.write(chunk)

        if verify_md5 and remote_md5_hex:
            local_md5 = (compute_md5_buffered(tmp) or "").lower()
            if local_md5 != remote_md5_hex:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                return (path_string, file_id, False, "md5_mismatch")

        os.replace(tmp, dest) 
        return (path_string, file_id, True, "downloaded_verified" if (verify_md5 and remote_md5_hex) else "downloaded")

    except Exception as e:
        try:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return (path_string, file_id, False, f"error:{e!r}")



def download_dataset(
    dataset_id: UUID,
    destination_path: str,
    include: list[str] = None,
    exclude: list[str] = None,
    verify_md5: bool = VERIFY_MD5,
):
    """Download files from a dataset with optional include/exclude filters.

    - include/exclude entries can be:
        * glob patterns applied to paths (e.g. '*/variants.vcf', '*.bam')
        * exact file IDs (UUID strings)
    """
    with console.status(
        f"[bold green]Preparing to download dataset [/bold green][bold cyan]{dataset_id}[/bold cyan][bold green] ...[/bold green]",
        spinner="dots12",
    ):
        try:
            datasets = client.get(f"/datasets?public_id={dataset_id}")
            
            if not datasets:
                console.print(
                    Panel(
                        f"[red]Dataset {dataset_id} not found.[/red]",
                        title="Error",
                        style="bold red",
                    )
                )
                return
            dataset = datasets[0]
            downloads_remaining = dataset.get("remaining_downloads")

            if downloads_remaining > 0:
                client.put(f"/datasets/{dataset_id}/decrement_downloads")
                console.print(
                    Panel(
                        f"[green]Dataset {dataset_id} has {downloads_remaining - 1} downloads remaining.[/green]",
                        title="Download Count",
                        style="bold green",
                    )
                )
            else:
                console.print(
                    Panel(
                        f"[yellow]Dataset {dataset_id} has no remaining downloads.[/yellow]",
                        title="Download Count",
                        style="bold yellow",
                    )
                )
                return

            total_size = int(dataset.get("total_size", 0) or 0)
            dgb = round(total_size / 1024**3, 2)

            files = dataset.get("adjusted_context", {})
            if not files:
                console.print(
                    Panel(
                        f"[yellow]Dataset {dataset_id} has no files to download.[/yellow]",
                        title="Notice",
                        style="bold",
                    )
                )
                return

            # Apply include/exclude filtering
            files = _filter_files_by_id_or_path(files, include, exclude)
            if not files:
                console.print(
                    Panel(
                        "[yellow]No files matched include/exclude filters.[/yellow]",
                        title="Filtered Out",
                        style="bold yellow",
                    )
                )
                return

            target = Path(destination_path).expanduser()
            avail = _available_bytes(target)
            required = _required_with_headroom(total_size) if total_size > 0 else None
            if total_size > 0 and avail < required:
                missing = required - avail
                console.print(
                    Panel(
                        "[red]Insufficient disk space.[/red]\n"
                        f"Required (with headroom): [bold]{_fmt_bytes(required)}[/bold]\n"
                        f"Available at target:       [bold]{_fmt_bytes(avail)}[/bold]\n"
                        f"Short by:                  [bold]{_fmt_bytes(missing)}[/bold]\n\n"
                        "Free up space or choose another destination and try again.",
                        title="Disk Space Check",
                        style="bold red",
                    )
                )
                return

            base_dir = target.resolve()
            base_dir.mkdir(parents=True, exist_ok=True)

            download_links = {}
            for path_string, file_id in files.items():
                fid = str(file_id)
                if fid in download_links:
                    continue
                download_links[fid] = client.post(
                    f"/temporary_links/{file_id}/request-download"
                )

        except Exception as e:
            console.print(
                Panel(
                    f"[red]Error fetching dataset {dataset_id}:[/red] {e}",
                    title="Error",
                    style="bold red",
                )
            )
            return

    total = len(files)
    outcome_counts = Counter()
    failures_details = []

    with console.status(
        f"[bold green]Downloading {total} files to {destination_path} (~{dgb} GiB) ...[/bold green]",
        spinner="dots12",
    ):
        pool = ThreadPoolExecutor(max_workers=FILE_WORKERS)
        try:
            futures = [
                pool.submit(
                    _download_one,
                    path_string,
                    str(file_id),
                    download_links.get(str(file_id)),
                    base_dir,
                    verify_md5,
                    PER_BLOB_CONCURRENCY,
                    CHUNK_SIZE,
                )
                for path_string, file_id in files.items()
            ]

            for fut in as_completed(futures):
                path_string, file_id, ok, note = fut.result()

                if ok:
                    if str(note).startswith("skipped"):
                        outcome_counts["skipped"] += 1
                    else:
                        outcome_counts["success"] += 1
                else:
                    outcome_counts["failed"] += 1
                    failures_details.append((path_string, note))

        except KeyboardInterrupt:
            console.print(
                Panel(
                    "[red]Download interrupted by user (Ctrl+C)[/red]",
                    title="Aborted",
                    style="bold red",
                )
            )
            pool.shutdown(cancel_futures=True)
            raise
        except Exception as e:
            failures_details.append(("__executor__", f"error:{e!r}"))
            outcome_counts["failed"] += 1
        finally:
            pool.shutdown(wait=True)

    if failures_details:
        for p, note in failures_details:
            console.print(
                Panel(f"[red]{note}[/red]\n{p}", title="File failed", style="bold red")
            )

    console.print(
        Panel(
            f"Success: [green]{outcome_counts.get('success', 0)}[/green]  •  "
            f"Skipped(existing): [yellow]{outcome_counts.get('skipped', 0)}[/yellow]  •  "
            f"Failed: [red]{outcome_counts.get('failed', 0)}[/red]",
            title="Download Summary",
            style="bold",
        )
    )

def list_datasets(n: int = None, sort_by: str = "created", ascending: bool = True):
    """
    List datasets with Rich output.

    Args:
        n (int, optional): Limit number of datasets displayed (after sorting).
        sort_by (str): Field to sort by: 'created', 'files', or 'size'.
        ascending (bool): Sort order; True=ascending, False=descending.
    """
    try:
    # if True: 
        with console.status("[bold cyan]Fetching datasets...[/bold cyan]", spinner="dots12"):
            my_datasets = client.get(
                "/datasets",
                params={
                    "include_jobs_as_input": False,
                    "include_jobs_as_output": False,
                    "include_context": False,
                    "include_adjusted_context": False,
                }
            )

        if not my_datasets:
            console.print(Panel("[yellow]No datasets found.[/yellow]", title="Datasets", style="bold"))
            return

        sort_key = (sort_by or "created").strip().lower()
        if sort_key not in {"created", "files", "size"}:
            console.print(Panel(
                f"[yellow]Unknown sort_by='{sort_by}'. Using 'created'.[/yellow]",
                title="Notice",
                style="bold"
            ))
            sort_key = "created"

        def key_func(d):
            if sort_key == "files":
                return d.get("number_of_files", 0)
            if sort_key == "size":
                return d.get("total_size", 0)
            dt = _parse_timestamp(d.get("created_at", ""))
            return dt or datetime.min 

        my_datasets = sorted(my_datasets, key=key_func, reverse=not ascending)
        if n is not None and n > 0:
            my_datasets = my_datasets[:n]

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


def _build_tree(files: dict[str, str]) -> dict:
    """Build nested dict with file_ids at leaves."""
    tree = {}
    for path, fid in files.items():
        parts = Path(path).parts
        node = tree
        for p in parts[:-1]:
            node = node.setdefault(p, {})
        node[parts[-1]] = {"__file_id__": fid}
    return tree


def _max_name_len(node: dict, depth: int = 0) -> int:
    """Recursively compute max filename length for alignment."""
    max_len = 0
    for key, child in node.items():
        if key == "__file_id__":
            continue
        length = len(key)
        if "__file_id__" in child:
            max_len = max(max_len, length)
        if isinstance(child, dict):
            max_len = max(max_len, _max_name_len(child, depth + 1))
    return max_len



def _print_tree(node: dict, prefix: str = "", level: int | None = None, depth: int = 0, name_pad: int | None = None):
    if name_pad is None:
        name_pad = _max_name_len(node)

    keys = sorted(node.keys())
    for i, key in enumerate(keys):
        if key == "__file_id__":
            continue
        connector = "└── " if i == len(keys) - 1 else "├── "
        entry = key
        file_id = node[key].get("__file_id__") if isinstance(node[key], dict) else None

        if file_id:
            pad_len = name_pad - len(entry)
            entry = f"{entry} {'─' * (pad_len + 4)} (file id: {file_id})"
        console.print(f"{prefix}{connector}{entry}")

        if isinstance(node[key], dict):
            if level is None or depth + 1 < level:
                extension = "    " if i == len(keys) - 1 else "│   "
                _print_tree(node[key], prefix + extension, level, depth + 1, name_pad)


def list_files_in_dataset(dataset_id: str, level: int | None = None):
    """List files in a dataset by its UUID, displaying hierarchy as a tree."""
    with console.status(f"[bold cyan]Fetching dataset {dataset_id}...[/bold cyan]", spinner="dots12"):
        datasets = client.get(f"/datasets?public_id={dataset_id}")
        if not datasets:
            console.print(Panel(f"[red]Dataset {dataset_id} not found.[/red]", title="Error", style="bold red"))
            return
        dataset = datasets[0]

    files = dataset.get("adjusted_context", {})
    if not files:
        console.print(Panel(f"[yellow]Dataset {dataset_id} has no files.[/yellow]", title="Notice", style="bold"))
        return

    tree = _build_tree(files)
    console.print(f"[bold green]Dataset {dataset_id} file tree:[/bold green]")
    _print_tree(tree, level=level)
