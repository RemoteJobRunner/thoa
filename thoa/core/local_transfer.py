import os
import time
from pathlib import Path
from typing import List

from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn

from thoa.core.api_utils import api_client
from thoa.core.input_specs import ParsedInputSpec
from thoa.core.job_utils import (
    collect_files,
    file_sizes_in_bytes,
    hash_all,
    upload_file_sas,
    console,
)


def create_mixed_dataset(specs: List[ParsedInputSpec], cwd: str) -> dict:
    """Upload a mixed set of local and Google Drive inputs as a single dataset.

    Handles arbitrary combinations of local files/directories and GDrive URLs,
    with optional ::mount_path suffixes on each spec. Returns:
        dataset_public_id: str | None
        input_context: {mount_path: file_public_id, ...}
    """
    from thoa.core.remote_inputs import (
        authorize_google_drive_transfer,
        extract_google_drive_folder_id,
        extract_google_drive_file_id,
        track_transfer_progress,
    )

    local_specs = [s for s in specs if s.kind == "local"]
    gdrive_specs = [s for s in specs if s.kind == "google_drive"]

    # Collect files per-spec so we can compute the correct mount_path for each file.
    # First spec to claim a file wins (handles overlapping source paths).
    spec_for_file: dict = {}
    for spec in local_specs:
        for f in collect_files([spec.source]):
            if f not in spec_for_file:
                spec_for_file[f] = spec

    all_local_files = list(spec_for_file.keys())
    file_sizes = file_sizes_in_bytes(all_local_files) if all_local_files else {}

    if all_local_files:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Hashing files", total=len(all_local_files))
            all_hashes = hash_all(all_local_files, on_progress=lambda: progress.advance(task))
    else:
        all_hashes = {}

    has_gdrive = bool(gdrive_specs)

    with console.status("Preparing dataset transfer...", spinner="dots12") as status:
        transfer = api_client.post("/data-transfers", json={
            "direction": "import",
            "remote_ref": {},
        })
        transfer_id = transfer["public_id"]

        # Authenticate once for all GDrive sources
        if has_gdrive:
            status.update("Authorizing Google Drive access...")
            authorize_google_drive_transfer(transfer_id)

        # Build unified manifest items
        manifest_items = []

        for file_path in all_local_files:
            path_str = str(file_path)
            spec = spec_for_file[file_path]

            if spec.mount_path is not None:
                resolved_base = os.path.normpath(os.path.join(cwd, spec.mount_path))
                source_abs = os.path.normpath(spec.source)
                if os.path.isdir(source_abs):
                    rel = os.path.relpath(path_str, source_abs)
                    item_mount = os.path.join(resolved_base, rel)
                else:
                    item_mount = os.path.join(resolved_base, os.path.basename(path_str))
            else:
                item_mount = path_str

            manifest_items.append({
                "provider": "local",
                "path": path_str,
                "md5": all_hashes[file_path],
                "size": file_sizes[file_path],
                "mount_path": item_mount,
            })

        for spec in gdrive_specs:
            folder_id = extract_google_drive_folder_id(spec.source)
            file_id = None if folder_id else extract_google_drive_file_id(spec.source)
            if not folder_id and not file_id:
                console.print(f"[bold red]Invalid Google Drive URL:[/bold red] {spec.source}")
                raise SystemExit(1)

            source_ref = {}
            if folder_id:
                source_ref["folder_id"] = folder_id
            else:
                source_ref["file_id"] = file_id

            item = {"provider": "google_drive", "source_ref": source_ref}
            if spec.mount_path:
                item["mount_path"] = spec.mount_path
            else:
                item["base_dir"] = cwd

            manifest_items.append(item)

        status.update("Building file manifest...")
        manifest_resp = api_client.post(
            f"/data-transfers/{transfer_id}/manifest/unified",
            json={"items": manifest_items},
        )
        if manifest_resp is None:
            raise SystemExit(1)

    # Check for mount path conflicts (server returns 409 on conflict)
    items = manifest_resp.get("items", [])
    to_upload = [item for item in items if item.get("upload_required") and item.get("provider") == "local"]
    n_local_skipped = sum(1 for item in items if not item.get("upload_required") and item.get("provider") == "local")
    n_remote_skipped = sum(1 for item in items if not item.get("upload_required") and item.get("provider") != "local")
    if n_local_skipped:
        console.print(f"  [dim]{n_local_skipped} local file(s) already uploaded, skipping.[/dim]")
    if n_remote_skipped:
        console.print(f"  [dim]{n_remote_skipped} remote file(s) already in storage, skipping.[/dim]")

    # Upload local files that need it
    path_str_to_local = {str(p): p for p in all_local_files}
    if to_upload:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Uploading files", total=len(to_upload))
            for item in to_upload:
                local_path = path_str_to_local.get(item["path"])
                if local_path is None:
                    console.print(f"[bold red]Error:[/bold red] Cannot find local file: {item['path']}")
                    raise SystemExit(1)
                upload_file_sas(local_path, item["upload_url"], all_hashes[local_path])
                api_client.post(
                    f"/data-transfers/{transfer_id}/import/items/{item['item_public_id']}/complete",
                    json={
                        "filename": item["path"],
                        "checksum": all_hashes[local_path],
                        "size": file_sizes[local_path],
                    },
                )
                progress.advance(task)

    with console.status("Starting dataset import...", spinner="dots12"):
        api_client.post(f"/data-transfers/{transfer_id}/start")

    final = track_transfer_progress(transfer_id, label="Importing data")

    if final.get("status") == "failed":
        transfer_view = api_client.get(f"/data-transfers/{transfer_id}")
        error = (transfer_view or {}).get("error_message") or "unknown error"
        raise RuntimeError(f"Dataset import failed: {error}")

    resolved = api_client.get(f"/data-transfers/{transfer_id}/resolved-context")

    return {
        "dataset_public_id": resolved["dataset_public_id"],
        "input_context": resolved["input_context"],
    }


def create_dataset(inputs: List[str]) -> dict:
    """Upload local files as a dataset via the local data-transfer import flow.

    Returns a dict with keys:
        dataset_public_id: str | None
        input_context: {relative_path: file_public_id, ...}
    """
    all_files = collect_files(inputs)
    if not all_files:
        return {"dataset_public_id": None, "input_context": {}}

    file_sizes = file_sizes_in_bytes(all_files)

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Hashing files", total=len(all_files))
        all_hashes = hash_all(all_files, on_progress=lambda: progress.advance(task))

    path_str_to_local = {str(p): p for p in all_files}

    with console.status("Preparing dataset transfer...", spinner="dots12") as status:
        transfer = api_client.post("/data-transfers", json={
            "provider": "local",
            "direction": "import",
            "remote_ref": {"provider": "local"},
        })
        transfer_id = transfer["public_id"]

        status.update("Building file manifest...")
        manifest_resp = api_client.post(
            f"/data-transfers/{transfer_id}/local/manifest",
            json={
                "files": [
                    {"path": str(p), "md5": all_hashes[p], "size": file_sizes[p]}
                    for p in all_files
                ]
            },
        )

    items = manifest_resp["items"]
    to_upload = [item for item in items if item["upload_required"]]
    n_skipped = len(items) - len(to_upload)
    if n_skipped:
        console.print(f"  [dim]{n_skipped} file(s) already uploaded, skipping.[/dim]")

    if to_upload:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Uploading files", total=len(to_upload))
            for item in to_upload:
                local_path = path_str_to_local.get(item["path"])
                upload_file_sas(local_path, item["upload_url"], all_hashes[local_path])
                api_client.post(
                    f"/data-transfers/{transfer_id}/import/items/{item['item_public_id']}/complete",
                    json={
                        "filename": item["path"],
                        "checksum": all_hashes[local_path],
                        "size": file_sizes[local_path],
                    },
                )
                progress.advance(task)

    with console.status("Finalizing dataset...", spinner="dots12"):
        api_client.post(f"/data-transfers/{transfer_id}/start")

        while True:
            manifest = api_client.get(
                f"/data-transfers/{transfer_id}/manifest",
                silent_status_codes={404},
            )
            if manifest and manifest.get("status") in ("completed", "failed"):
                break
            time.sleep(2)

        if manifest.get("status") == "failed":
            transfer_view = api_client.get(f"/data-transfers/{transfer_id}")
            error = (transfer_view or {}).get("error_message") or "unknown error"
            raise RuntimeError(f"Local dataset import failed: {error}")

        resolved = api_client.get(f"/data-transfers/{transfer_id}/resolved-context")

    return {
        "dataset_public_id": resolved["dataset_public_id"],
        "input_context": resolved["input_context"],
    }
