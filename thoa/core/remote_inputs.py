import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Event
from urllib.parse import parse_qs, urlparse

import typer

from thoa.config import settings
from thoa.core.api_utils import api_client
from thoa.core.job_utils import console


def detect_input_source_kind(inputs: list[str]) -> str:
    kinds = set()
    for raw in inputs:
        value = str(raw).strip()
        if extract_google_drive_folder_id(value):
            kinds.add("google_drive")
        elif value.startswith("s3://"):
            kinds.add("s3")
        else:
            kinds.add("local")
    if not kinds:
        return "none"
    if len(kinds) > 1:
        return "mixed"
    return next(iter(kinds))


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


def google_drive_redirect_uri() -> str:
    return (
        f"http://{settings.THOA_GDRIVE_CALLBACK_HOST}:"
        f"{settings.THOA_GDRIVE_CALLBACK_PORT}/google-drive/callback"
    )


def _wait_for_google_callback(expected_state: str, timeout_seconds: int = 300) -> str:
    event = Event()
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
            event.set()

        def log_message(self, format, *args):
            return

    server = HTTPServer(
        (settings.THOA_GDRIVE_CALLBACK_HOST, settings.THOA_GDRIVE_CALLBACK_PORT),
        CallbackHandler,
    )
    server.timeout = 1
    started = time.time()
    try:
        while not event.is_set() and (time.time() - started) < timeout_seconds:
            server.handle_request()
    finally:
        server.server_close()

    if payload.get("error"):
        console.print(f"[bold red]Google authorization failed:[/bold red] {payload['error']}")
        raise typer.Exit(code=1)
    if not payload.get("code"):
        console.print("[bold red]Timed out waiting for Google Drive authorization callback.[/bold red]")
        raise typer.Exit(code=1)
    if payload.get("state") != expected_state:
        console.print("[bold red]Google authorization state mismatch.[/bold red]")
        raise typer.Exit(code=1)
    return str(payload["code"])


def import_google_drive_input(folder_url: str) -> str:
    folder_id = extract_google_drive_folder_id(folder_url)
    if not folder_id:
        console.print("[bold red]Invalid Google Drive folder URL.[/bold red]")
        raise typer.Exit(code=1)

    transfer = api_client.post(
        "/data-transfers",
        json={
            "provider": "google_drive",
            "direction": "import",
            "source_ref": {
                "provider": "google_drive",
                "folder_id": folder_id,
            },
        },
    )
    if not transfer:
        raise typer.Exit(code=1)

    transfer_id = transfer["public_id"]
    redirect_uri = google_drive_redirect_uri()

    auth_start = api_client.post(
        f"/data-transfers/{transfer_id}/google-drive/auth/start",
        json={"redirect_uri": redirect_uri},
    )
    if not auth_start:
        raise typer.Exit(code=1)

    auth_url = auth_start["auth_url"]
    state = auth_start["state"]

    console.print("[bold cyan]Starting Google Drive authorization...[/bold cyan]")
    console.print(f"[dim]{auth_url}[/dim]")
    if settings.THOA_GDRIVE_OPEN_BROWSER:
        webbrowser.open(auth_url)

    code = _wait_for_google_callback(expected_state=state)

    auth_complete = api_client.post(
        f"/data-transfers/{transfer_id}/google-drive/auth/complete",
        json={
            "code": code,
            "redirect_uri": redirect_uri,
        },
    )
    if not auth_complete:
        raise typer.Exit(code=1)

    manifest_status = api_client.post(f"/data-transfers/{transfer_id}/manifest")
    if not manifest_status:
        raise typer.Exit(code=1)

    manifest = api_client.get(f"/data-transfers/{transfer_id}/manifest")
    if manifest:
        console.print(
            f"[green]Google Drive manifest ready:[/green] "
            f"{manifest['total_items']} items, {manifest['total_bytes']} bytes"
        )

    start_status = api_client.post(f"/data-transfers/{transfer_id}/start")
    if not start_status:
        raise typer.Exit(code=1)

    with console.status("Importing Google Drive data", spinner="dots12"):
        while True:
            status = api_client.get(f"/data-transfers/{transfer_id}")
            if not status:
                raise typer.Exit(code=1)
            if status["status"] == "completed":
                dataset_public_id = status.get("dataset_public_id")
                if not dataset_public_id:
                    console.print("[bold red]Transfer completed without dataset id.[/bold red]")
                    raise typer.Exit(code=1)
                return dataset_public_id
            if status["status"] == "failed":
                console.print(
                    f"[bold red]Google Drive import failed:[/bold red] {status.get('error_message') or 'unknown error'}"
                )
                raise typer.Exit(code=1)
            time.sleep(4)
