import os
import queue
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import typer

from thoa.config import settings
from thoa.core.api_utils import api_client
from thoa.core.job_utils import console


def detect_input_source_kind(value: str | None) -> str:
    if not value:
        return "none"
    value = str(value).strip()
    if extract_google_drive_folder_id(value) or extract_google_drive_file_id(value):
        return "google_drive"
    if value.startswith("s3://"):
        return "s3"
    return "unknown"


def detect_remote_ref_kind(value: str | None) -> str:
    if not value:
        return "none"
    value = str(value).strip()
    if extract_google_drive_folder_id(value):
        return "google_drive"
    if value.startswith("s3://"):
        return "s3"
    return "unknown"


def project_input_context(input_root: str, input_context: dict[str, object]) -> dict[str, object]:
    root = input_root.rstrip("/") or "/"
    projected = {}
    for path, file_id in input_context.items():
        rel_path = str(path).lstrip("/")
        projected_path = f"{root}/{rel_path}" if root != "/" else f"/{rel_path}"
        projected[projected_path] = file_id
    return projected


def extract_google_drive_folder_id(value: str) -> str | None:
    parsed = urlparse(value)
    if parsed.netloc not in {"drive.google.com", "www.drive.google.com"}:
        return None

    parts = [part for part in parsed.path.split("/") if part]
    if "folders" in parts:
        idx = parts.index("folders")
        if idx + 1 < len(parts):
            return parts[idx + 1]

    query_id = parse_qs(parsed.query).get("id")
    if query_id:
        return query_id[0]

    return None


def extract_google_drive_file_id(value: str) -> str | None:
    """Return the file id from a Drive single-file sharing link.

    Recognizes ``https://drive.google.com/file/d/<FILE_ID>/view`` style URLs.
    Folder links return None (use ``extract_google_drive_folder_id``).
    """
    parsed = urlparse(value)
    if parsed.netloc not in {"drive.google.com", "www.drive.google.com"}:
        return None

    parts = [part for part in parsed.path.split("/") if part]
    if "file" in parts:
        idx = parts.index("file")
        if idx + 2 < len(parts) and parts[idx + 1] == "d":
            return parts[idx + 2]

    return None


def google_drive_redirect_uri() -> str:
    return (
        f"http://{settings.THOA_GDRIVE_CALLBACK_HOST}:"
        f"{settings.THOA_GDRIVE_CALLBACK_PORT}/google-drive/callback"
    )


def _parse_pasted_callback(pasted: str) -> tuple[str | None, str | None, str | None]:
    """Extract (code, state, error) from a pasted callback URL or query string."""
    pasted = pasted.strip().strip('"').strip("'")
    if not pasted:
        return None, None, None
    query = urlparse(pasted).query if "://" in pasted else pasted.lstrip("?")
    params = parse_qs(query)
    return (
        (params.get("code") or [None])[0],
        (params.get("state") or [None])[0],
        (params.get("error") or [None])[0],
    )


def _await_google_drive_code(expected_state: str, timeout_seconds: int = 300) -> str:
    """Wait for the auth code via two concurrent paths; first one wins.

    1. Loopback HTTP listener (laptop case — browser hits 127.0.0.1).
    2. Pasted redirect URL on stdin (SSH / headless case — browser can't reach loopback).
    On non-POSIX or non-tty stdin, only the listener is active.
    """
    payload: dict[str, str | None] = {}

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path != "/google-drive/callback":
                self.send_response(404)
                self.end_headers()
                return
            query = parse_qs(parsed.query)
            payload["code"] = query.get("code", [None])[0]
            payload["state"] = query.get("state", [None])[0]
            payload["error"] = query.get("error", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.end_headers()
            self.wfile.write(b"Google Drive authorization complete. You can close this tab.")

        def log_message(self, format, *args):
            return

    server = HTTPServer(
        (settings.THOA_GDRIVE_CALLBACK_HOST, settings.THOA_GDRIVE_CALLBACK_PORT),
        CallbackHandler,
    )
    server.timeout = 0.5

    can_read_stdin = os.name == "posix"
    stdin_queue: queue.Queue = queue.Queue()

    def _stdin_reader():
        try:
            for line in sys.stdin:
                stdin_queue.put(line)
        except Exception:
            pass

    if can_read_stdin:
        console.print(
            "[cyan]Waiting for browser callback.[/cyan] "
            "[dim]If your browser shows 'site can't be reached'"
            ", paste the URL it tried to load and press Enter:[/dim]"
        )
        sys.stdout.write("> ")
        sys.stdout.flush()
        threading.Thread(target=_stdin_reader, daemon=True).start()

    deadline = time.time() + timeout_seconds
    try:
        while time.time() < deadline:
            server.handle_request()

            if payload.get("error"):
                console.print(
                    f"[bold red]Google authorization failed:[/bold red] {payload['error']}"
                )
                raise typer.Exit(code=1)
            if payload.get("code"):
                if payload.get("state") != expected_state:
                    console.print("[bold red]Google authorization state mismatch.[/bold red]")
                    raise typer.Exit(code=1)
                return str(payload["code"])

            try:
                line = stdin_queue.get_nowait()
            except queue.Empty:
                line = None
            if line is not None:
                code, state, error = _parse_pasted_callback(line)
                if error:
                    console.print(
                        f"[bold red]Google authorization failed:[/bold red] {error}"
                    )
                    raise typer.Exit(code=1)
                if code and state:
                    if state != expected_state:
                        console.print("[bold red]Google authorization state mismatch.[/bold red]")
                        raise typer.Exit(code=1)
                    return code
                if line.strip():
                    console.print(
                        "[yellow]Could not find `code` and `state` in that input. "
                        "Paste the full URL the browser tried to load (starts with "
                        "http://127.0.0.1:...).[/yellow]"
                    )
                sys.stdout.write("> ")
                sys.stdout.flush()
    finally:
        server.server_close()

    console.print("[bold red]Timed out waiting for Google Drive authorization.[/bold red]")
    raise typer.Exit(code=1)


def authorize_google_drive_transfer(transfer_id: str) -> dict[str, object]:
    redirect_uri = google_drive_redirect_uri()

    auth_start = api_client.post(
        f"/data-transfers/{transfer_id}/google-drive/auth/start",
        json={"redirect_uri": redirect_uri},
    )
    if not auth_start:
        raise typer.Exit(code=1)

    auth_url = auth_start["auth_url"]
    state = auth_start["state"]

    console.print()
    console.print("[bold cyan]Google Drive authorization[/bold cyan]")
    console.print("Open this URL in a browser and approve access:")
    console.print()
    console.print(f"  [link={auth_url}]{auth_url}[/link]")
    console.print()

    opened = False
    if settings.THOA_GDRIVE_OPEN_BROWSER:
        try:
            opened = webbrowser.open(auth_url)
        except Exception:
            opened = False
    if opened:
        console.print("[dim](We tried to open it for you — if nothing appeared, copy the URL above.)[/dim]")
    else:
        console.print("[yellow]Could not open a browser automatically — copy the URL above into one.[/yellow]")

    code = _await_google_drive_code(expected_state=state)

    auth_complete = api_client.post(
        f"/data-transfers/{transfer_id}/google-drive/auth/complete",
        json={
            "code": code,
            "redirect_uri": redirect_uri,
        },
    )
    if not auth_complete:
        raise typer.Exit(code=1)

    return auth_complete


def import_google_drive_input(
    source_url: str,
    *,
    retain_credential_for_export: bool = False,
    defer_execution: bool = False,
) -> dict[str, object]:
    folder_id = extract_google_drive_folder_id(source_url)
    file_id = None if folder_id else extract_google_drive_file_id(source_url)
    if not folder_id and not file_id:
        console.print("[bold red]Invalid Google Drive URL.[/bold red]")
        raise typer.Exit(code=1)

    remote_ref: dict[str, object] = {"provider": "google_drive"}
    if folder_id:
        remote_ref["folder_id"] = folder_id
    else:
        remote_ref["file_id"] = file_id

    transfer = api_client.post(
        "/data-transfers",
        json={
            "provider": "google_drive",
            "direction": "import",
            "remote_ref": remote_ref,
            "retain_credential_for_export": retain_credential_for_export,
        },
    )
    if not transfer:
        raise typer.Exit(code=1)

    transfer_id = transfer["public_id"]
    auth_complete = authorize_google_drive_transfer(transfer_id)
    if not auth_complete:
        raise typer.Exit(code=1)

    manifest_status = api_client.post(f"/data-transfers/{transfer_id}/manifest")
    if not manifest_status:
        raise typer.Exit(code=1)

    manifest = api_client.get(f"/data-transfers/{transfer_id}/manifest")
    if manifest:
        total_items = manifest["total_items"]
        skipped_count = manifest.get("skipped_items", 0)
        importable = total_items - skipped_count
        console.print(
            f"[green]Google Drive manifest ready:[/green] "
            f"{importable} importable item(s), {manifest['total_bytes']} bytes"
        )
        if skipped_count:
            console.print(
                f"[yellow]Skipping {skipped_count} unsupported item(s):[/yellow]"
            )
            samples = manifest.get("skipped_samples", [])
            for sample in samples:
                console.print(
                    f"  [dim]- {sample['path']} "
                    f"({sample.get('mime_type') or 'unknown type'})[/dim]"
                )
            if len(samples) < skipped_count:
                console.print(
                    f"  [dim]... and {skipped_count - len(samples)} more[/dim]"
                )

    if defer_execution:
        return {
            "transfer_public_id": transfer_id,
            "status": manifest_status.get("status"),
            "manifest": manifest,
        }

    start_status = api_client.post(f"/data-transfers/{transfer_id}/start")
    if not start_status:
        raise typer.Exit(code=1)

    with console.status("Importing Google Drive data", spinner="dots12"):
        while True:
            status = api_client.get(f"/data-transfers/{transfer_id}")
            if not status:
                raise typer.Exit(code=1)
            if status["status"] == "completed":
                resolved = api_client.get(f"/data-transfers/{transfer_id}/resolved-context")
                if not resolved:
                    raise typer.Exit(code=1)
                dataset_public_id = resolved.get("dataset_public_id")
                if not dataset_public_id:
                    console.print("[bold red]Transfer completed without dataset id.[/bold red]")
                    raise typer.Exit(code=1)
                return resolved
            if status["status"] == "failed":
                console.print(
                    f"[bold red]Google Drive import failed:[/bold red] {status.get('error_message') or 'unknown error'}"
                )
                raise typer.Exit(code=1)
            time.sleep(4)
