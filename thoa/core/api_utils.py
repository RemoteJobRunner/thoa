import httpx
from typing import Optional
from thoa.config import settings
from rich import print as rprint

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
        
        response = self.client.request(method, path, **kwargs)
        
        if response.status_code == 200: 
            return response.json()
        else:
            ErrorReadouts(response.status_code, response.json().get("detail")).readout()
            return

    def get(self, path: str, **kwargs):
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs):
        return self._request("POST", path, **kwargs)

    def close(self):
        self.client.close()

api_client = ApiClient(
    base_url=settings.THOA_API_URL,
    api_key=settings.THOA_API_KEY,
    timeout=settings.THOA_API_TIMEOUT,
)