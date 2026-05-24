import httpx
from typing import Optional
from thoa.config import settings
from rich import print as rprint
import asyncio, json, websockets 
from rich.console import Console
from rich.text import Text

console = Console()

class ErrorReadouts: 
    def __init__(self, status_code: int, detail: Optional[str] = None):
        self.status_code = status_code
        self.detail = detail

    def readout(self):
        if self.status_code == 403:
            rprint("[bold red]403 Forbidden: You're not allowed to access this resource.[/bold red]\n\n"
               "[yellow]HINT: Have you set your API key in the environment variable THOA_API_KEY\n"
               "(e.g. 'echo $THOA_API_KEY')?[/yellow]")
            
        elif self.status_code == 401: 
            rprint("[bold red]401 Unauthorized: Authentication is required and has failed or has not yet been provided.[/bold red]\n\n"
               "[yellow]HINT: Have you set your API key in the environment variable THOA_API_KEY\n"
               "(e.g. 'echo $THOA_API_KEY')?[/yellow]")
            
        elif self.status_code == 400: 
            rprint("[bold red]400 Bad Request: The request was invalid or cannot be served.[/bold red]\n\n"
               f"[yellow]SERVER MESSAGE:\n{self.detail}[/yellow]")

        elif self.status_code == 500:
            rprint("[bold red]500 Internal Server Error: The server encountered an unexpected condition that prevented it from fulfilling the request.[/bold red]\n\n"
               "[yellow]HINT: This is likely a server-side issue. Please try again later or contact support.[/yellow]")

        else: 
            rprint(f"[bold red]{self.status_code} Error: An unexpected error occurred.[/bold red]\n\n"
               f"[yellow]SERVER MESSAGE:\n{self.detail}[/yellow]")

class ApiClient:
    def __init__(self, base_url: str, api_key: Optional[str] = None, timeout: int = 10):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.client = httpx.Client(
            base_url=self.base_url,
            headers={
                "X-API-Key": self.api_key if self.api_key else "",
                "Accept": "application/json",
            },
            timeout=httpx.Timeout(timeout),
        )

    def _request(
        self,
        method: str,
        path: str,
        *,
        silent_status_codes: set[int] | None = None,
        **kwargs,
    ):

        if not self.api_key:
            rprint("[bold red]ERROR: No API key provided. Please set the THOA_API_KEY environment variable.[/bold red]\n")
            rprint(f"You can obtain an API key from the THOA web interface at [blue]{settings.THOA_UI_URL}/workbench/api_keys[/blue]")
            return

        api_path = f"/api{path}"
        response = self.client.request(method, api_path, **kwargs)

        if response.status_code == 200:
            if settings.THOA_API_DEBUG:
                rprint(f"[green]DEBUG: Successful {method} request to {api_path}[/green]")
                rprint(f"[green]Response:[/green] {response.json()}")
            return response.json()
        else:
            if silent_status_codes and response.status_code in silent_status_codes:
                return None
            detail = None
            try:
                payload = response.json()
                if isinstance(payload, dict):
                    detail = payload.get("detail")
                else:
                    detail = str(payload)
            except Exception:
                detail = response.text.strip() or None
            ErrorReadouts(response.status_code, detail).readout()
            return

    def get(self, path: str, *, silent_status_codes: set[int] | None = None, **kwargs):
        return self._request("GET", path, silent_status_codes=silent_status_codes, **kwargs)

    def post(self, path: str, *, silent_status_codes: set[int] | None = None, **kwargs):
        return self._request("POST", path, silent_status_codes=silent_status_codes, **kwargs)

    def put(self, path: str, *, silent_status_codes: set[int] | None = None, **kwargs):
        return self._request("PUT", path, silent_status_codes=silent_status_codes, **kwargs)

    def close(self):
        self.client.close()

    async def stream_logs(self, job_id: str, from_id: str = "$") -> bool:
        """Stream logs for the current attempt of job_id.

        Returns True if the attempt succeeded, False if it failed.
        Prints lines as they arrive; the caller is responsible for
        printing the per-attempt success/failure message.
        """
        base = self.base_url
        if base.startswith("https://"):
            ws_base = "wss://" + base[len("https://"):]
        elif base.startswith("http://"):
            ws_base = "ws://" + base[len("http://"):]
        else:
            ws_base = "ws://" + base.lstrip("/")

        url = f"{ws_base}/ws/logs/{job_id}?from_id={from_id}"
        headers = {
            "X-API-Key": self.api_key or "",
            "Accept": "application/json",
        }

        succeeded = False
        async with websockets.connect(
            url,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=20
        ) as ws:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    console.print(raw)
                    continue

                if msg.get("event") == "keepalive":
                    continue

                if msg.get("event") == "connected":
                    continue

                if msg.get("event") == "cancelled":
                    console.print("[bold red]Job was cancelled.[/bold red]")
                    await ws.close()
                    break

                if msg.get("event") == "error":
                    console.print(f"[red]error:[/red] {msg.get('message')}")
                    break

                if msg.get("event") == "done":
                    succeeded = msg.get("success") == 1
                    await ws.close()
                    break

                # Standard log entries
                stream = msg.get("stream")
                data = msg.get("data", "")

                if stream == "stderr":
                    console.print(f"[orange3][remote stderr][/orange3] {data}", end="")
                else:
                    console.print(f"[blue][remote stdout][/blue] {data}", end="")

        return succeeded

    def stream_logs_blocking(self, job_id: str, from_id: str = "0-0") -> bool:
        """Convenience wrapper for sync CLIs. Returns True on success."""
        return asyncio.run(self.stream_logs(job_id, from_id))

api_client = ApiClient(
    base_url=settings.THOA_API_URL,
    api_key=settings.THOA_API_KEY,
    timeout=settings.THOA_API_TIMEOUT,
)