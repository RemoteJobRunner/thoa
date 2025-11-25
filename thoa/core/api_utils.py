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
            
        elif self.status_code == 400: 
            rprint("[bold red]400 Bad Request: The request was invalid or cannot be served.[/bold red]\n\n"
               f"[yellow]SERVER MESSAGE:\n{self.detail}[/yellow]")
            
        elif self.status_code == 500:
            rprint("[bold red]500 Internal Server Error: The server encountered an unexpected condition that prevented it from fulfilling the request.[/bold red]\n\n"
               "[yellow]HINT: This is likely a server-side issue. Please try again later or contact support.[/yellow]")

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

    def _request(self, method: str, path: str, **kwargs):
        
        api_path = f"/api{path}"
        response = self.client.request(method, api_path, **kwargs)
        
        if response.status_code == 200: 
            return response.json()
        else:
            ErrorReadouts(response.status_code, response.json().get("detail")).readout()
            return

    def get(self, path: str, **kwargs):
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs):
        return self._request("POST", path, **kwargs)

    def put(self, path: str, **kwargs):
        return self._request("PUT", path, **kwargs)

    def close(self):
        self.client.close()

    async def stream_logs(self, job_id: str, from_id: str = "$"):
        """
        Connects to ws://<base>/ws/logs/{job_id}?from_id=<from_id>
        Sends X-API-Key and the same Accept header as HTTP client.
        Prints lines as they arrive.
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
                    console.print(f"[green]connected[/green] job={msg.get('job_id')} from_id={msg.get('from_id')}")
                    continue

                if msg.get("event") == "error":
                    console.print(f"[red]error:[/red] {msg.get('message')}")
                    break

                if msg.get("event") == "done":
                    if msg.get("success") == 1:
                        console.print("[bold green] Job succeeded [/bold green]")
                    else:
                        console.print("[bold red] Job failed [/bold red]")
                    await ws.close()
                    break

                # Standard log entries
                stream = msg.get("stream")
                data = msg.get("data", "")

                if stream == "stderr":
                    console.print(f"[orange3][remote stderr][/orange3] {data}", end="")
                else:
                    console.print(f"[blue][remote stdout][/blue] {data}", end="")

    def stream_logs_blocking(self, job_id: str, from_id: str = "0-0"):
        """Convenience wrapper for sync CLIs."""
        asyncio.run(self.stream_logs(job_id, from_id))

api_client = ApiClient(
    base_url=settings.THOA_API_URL,
    api_key=settings.THOA_API_KEY,
    timeout=settings.THOA_API_TIMEOUT,
)