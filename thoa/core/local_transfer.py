import time
from pathlib import Path
from typing import List

from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn

from thoa.core.api_utils import api_client
from thoa.core.job_utils import (
    collect_files,
    file_sizes_in_bytes,
    hash_all,
    upload_file_sas,
    console,
)


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
