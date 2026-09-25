import os
from dataclasses import dataclass, field
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
from thoa.core.remote_inputs import PUBLIC_PROVIDERS


@dataclass
class PreparedTransfer:
    """An import transfer whose manifest exists but whose data hasn't moved yet."""

    transfer_id: str
    manifest: dict
    local_files: list = field(default_factory=list)
    hashes: dict = field(default_factory=dict)
    sizes: dict = field(default_factory=dict)

    @property
    def items(self) -> list:
        return self.manifest.get("items", [])

    @property
    def has_public_items(self) -> bool:
        return any(item.get("provider") in PUBLIC_PROVIDERS for item in self.items)


def _format_bytes(n: int) -> str:
    for unit, scale in (("TB", 1024 ** 4), ("GB", 1024 ** 3), ("MB", 1024 ** 2), ("KB", 1024)):
        if n >= scale:
            return f"{n / scale:.1f} {unit}"
    return f"{n} B"


def prepare_mixed_transfer(specs: List[ParsedInputSpec], cwd: str) -> PreparedTransfer:
    """Create the transfer and its manifest; nothing is uploaded or started yet.

    Public accessions are resolved server-side here, so bad accessions,
    colliding paths and quota problems all surface before any bytes move.
    """
    from thoa.core.remote_inputs import (
        authorize_google_drive_transfer,
        extract_google_drive_folder_id,
        extract_google_drive_file_id,
    )

    local_specs = [s for s in specs if s.kind == "local"]
    gdrive_specs = [s for s in specs if s.kind == "google_drive"]
    public_specs = [s for s in specs if s.kind in PUBLIC_PROVIDERS]

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
        if transfer is None:
            raise SystemExit(1)
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

        for spec in public_specs:
            item = {"provider": spec.kind, "source_ref": {"accession": spec.source}}
            if spec.mount_path:
                # Relative mount paths are relative to cwd, like local inputs.
                item["mount_path"] = os.path.normpath(os.path.join(cwd, spec.mount_path))
            else:
                item["base_dir"] = cwd
            manifest_items.append(item)

        status.update(
            "Resolving public accessions and building file manifest..."
            if public_specs else "Building file manifest..."
        )
        manifest_resp = api_client.post(
            f"/data-transfers/{transfer_id}/manifest/unified",
            json={"items": manifest_items},
        )
        if manifest_resp is None:
            raise SystemExit(1)

    # Check for mount path conflicts (server returns 409 on conflict)
    items = manifest_resp.get("items", [])
    n_local_skipped = sum(1 for item in items if not item.get("upload_required") and item.get("provider") == "local")
    n_remote_skipped = sum(1 for item in items if not item.get("upload_required") and item.get("provider") != "local")
    if public_specs:
        public_items = [item for item in items if item.get("provider") in PUBLIC_PROVIDERS]
        public_bytes = sum(item.get("size") or 0 for item in public_items)
        console.print(
            f"[green]Resolved {len(public_specs)} public accession(s):[/green] "
            f"{len(public_items)} file(s), {_format_bytes(public_bytes)}"
        )
    if n_local_skipped:
        console.print(f"  [dim]{n_local_skipped} local file(s) already uploaded, skipping.[/dim]")
    if n_remote_skipped:
        console.print(f"  [dim]{n_remote_skipped} remote file(s) already in storage, skipping.[/dim]")

    return PreparedTransfer(
        transfer_id=transfer_id,
        manifest=manifest_resp,
        local_files=all_local_files,
        hashes=all_hashes,
        sizes=file_sizes,
    )


def upload_local_items(prepared: PreparedTransfer) -> None:
    """Upload the local files the manifest still needs."""
    to_upload = [item for item in prepared.items if item.get("upload_required") and item.get("provider") == "local"]
    path_str_to_local = {str(p): p for p in prepared.local_files}
    if not to_upload:
        return
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
            upload_file_sas(local_path, item["upload_url"], prepared.hashes[local_path])
            api_client.post(
                f"/data-transfers/{prepared.transfer_id}/import/items/{item['item_public_id']}/complete",
                json={
                    "filename": item["path"],
                    "checksum": prepared.hashes[local_path],
                    "size": prepared.sizes[local_path],
                },
            )
            progress.advance(task)


def start_transfer(prepared: PreparedTransfer) -> None:
    with console.status("Starting dataset import...", spinner="dots12"):
        api_client.post(f"/data-transfers/{prepared.transfer_id}/start")


def track_transfer(prepared: PreparedTransfer) -> dict:
    """Follow the import until it ends; returns {dataset_public_id, input_context}.

    Raises RuntimeError with the server's reason if the import failed.
    """
    from thoa.core.remote_inputs import track_transfer_progress

    final = track_transfer_progress(
        prepared.transfer_id, label="Importing data", by_bytes=prepared.has_public_items,
    )

    if final.get("status") in ("failed", "cancelled"):
        transfer_view = api_client.get(f"/data-transfers/{prepared.transfer_id}")
        error = (transfer_view or {}).get("error_message") or final.get("status") or "unknown error"
        raise RuntimeError(f"Dataset import failed: {error}")

    resolved = api_client.get(f"/data-transfers/{prepared.transfer_id}/resolved-context")

    return {
        "dataset_public_id": resolved["dataset_public_id"],
        "input_context": resolved["input_context"],
    }


def create_mixed_dataset(specs: List[ParsedInputSpec], cwd: str) -> dict:
    """Upload a mixed set of local and Google Drive inputs as a single dataset.

    Handles arbitrary combinations of local files/directories and GDrive URLs,
    with optional ::mount_path suffixes on each spec. Returns:
        dataset_public_id: str | None
        input_context: {mount_path: file_public_id, ...}
    """
    prepared = prepare_mixed_transfer(specs, cwd)
    upload_local_items(prepared)
    start_transfer(prepared)
    return track_transfer(prepared)
