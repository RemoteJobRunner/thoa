import threading
import webbrowser
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
from uuid import uuid4

from datetime import datetime

import typer
from rich.console import Console

from thoa.config import settings
from thoa.core.credentials import read_credentials, write_credentials
from thoa.core.api_utils import ApiClient

console = Console()


def _fmt_expiry(iso: str | None) -> str:
    if not iso:
        return "N/A"
    try:
        return datetime.fromisoformat(iso).strftime("%-d %b %Y at %H:%M")
    except ValueError:
        return iso


LOGIN_TIMEOUT_SECONDS = 300
CALLBACK_PORT_START = 9700
CALLBACK_PORT_END = 9800


def _find_free_port() -> int:
    import socket
    for port in range(CALLBACK_PORT_START, CALLBACK_PORT_END):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("No free port found in range 9700-9800")


def _check_existing_session() -> bool:
    """Return True if stored credentials are present and the key is still valid on the server."""
    creds = read_credentials()
    if not creds or not creds.get("api_key"):
        return False

    client = ApiClient(base_url=settings.THOA_API_URL, api_key=creds["api_key"], timeout=10)
    result = client.get("/api_keys", silent_status_codes={401, 403, 426})
    client.close()

    if result is not None:
        email = creds.get("email") or "unknown"
        console.print(f"[green]Already logged in as[/green] [bold]{email}[/bold]")
        console.print(f"[dim]Token expires: {_fmt_expiry(creds.get('expires_at'))}[/dim]")
        return True

    return False


def login():
    if _check_existing_session():
        return

    state = str(uuid4())
    try:
        port = _find_free_port()
    except RuntimeError:
        console.print(
            f"[bold red]Login failed:[/bold red] no free port found in range "
            f"{CALLBACK_PORT_START}–{CALLBACK_PORT_END}. "
            "Free a port or restart your terminal and try again."
        )
        raise typer.Exit(code=1)

    received: dict = {}
    server_error: list = []

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path != "/callback":
                self.send_response(404)
                self.end_headers()
                return

            params = dict(urllib.parse.parse_qsl(parsed.query))
            returned_state = params.get("state")
            code = params.get("code")

            if returned_state != state:
                server_error.append("State mismatch — possible CSRF attempt.")
                self._respond(400, "Authentication failed: state mismatch.")
                return

            if not code:
                server_error.append("No exchange code in callback.")
                self._respond(400, "Authentication failed: missing code.")
                return

            received["code"] = code
            self._respond(200, (
                "<html><body style='font-family:sans-serif;text-align:center;margin-top:80px'>"
                "<h2>Authenticated!</h2>"
                "<p>You can close this tab and return to your terminal.</p>"
                "</body></html>"
            ), content_type="text/html")

        def _respond(self, status: int, body: str, content_type: str = "text/plain"):
            encoded = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, *args):
            pass  # silence access logs

    httpd = HTTPServer(("127.0.0.1", port), CallbackHandler)
    httpd.timeout = LOGIN_TIMEOUT_SECONDS

    url = (
        f"{settings.THOA_UI_URL}/cli-auth"
        f"?state={urllib.parse.quote(state)}&port={port}"
    )

    console.print(f"[bold]Opening browser to complete login...[/bold]")
    console.print(f"If the browser does not open, visit:\n[blue]{url}[/blue]\n")
    webbrowser.open(url)

    httpd.handle_request()

    if server_error:
        console.print(f"[bold red]Login failed:[/bold red] {server_error[0]}")
        raise typer.Exit(code=1)

    if "code" not in received:
        console.print("[bold red]Login timed out.[/bold red] Please run [bold]thoa login[/bold] again.")
        raise typer.Exit(code=1)

    exchange_code = received["code"]

    console.print("Exchanging token...", end="")

    client = ApiClient(base_url=settings.THOA_API_URL, api_key=None, timeout=15)
    data = client.get("/auth/cli-exchange", require_auth=False, params={"code": exchange_code})
    client.close()

    if data is None:
        console.print("\n[bold red]Failed to exchange token.[/bold red] Please run [bold]thoa login[/bold] again.")
        raise typer.Exit(code=1)

    existing = read_credentials()
    if existing and existing.get("public_id"):
        _revoke_old_key(existing["public_id"], existing.get("api_key"))

    write_credentials(data)

    email = data.get("email") or "unknown"
    console.print(f"\n[green]Logged in as[/green] [bold]{email}[/bold]")
    console.print(f"[dim]Token expires: {_fmt_expiry(data.get('expires_at'))}[/dim]")


def _revoke_old_key(public_id: str, api_key: str | None):
    if not api_key:
        return
    try:
        client = ApiClient(base_url=settings.THOA_API_URL, api_key=api_key, timeout=10)
        client.delete(f"/api_keys/{public_id}", silent_status_codes={404})
        client.close()
    except Exception:
        pass
