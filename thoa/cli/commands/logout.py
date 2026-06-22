import typer
from rich.console import Console

from thoa.config import settings
from thoa.core.credentials import read_credentials, delete_credentials
from thoa.core.api_utils import ApiClient

console = Console()


def logout():
    creds = read_credentials()

    if not creds:
        console.print("[yellow]Not logged in.[/yellow]")
        raise typer.Exit(code=0)

    public_id = creds.get("public_id")
    api_key = creds.get("api_key")

    if public_id and api_key:
        try:
            client = ApiClient(base_url=settings.THOA_API_URL, api_key=api_key, timeout=10)
            client.delete(f"/api_keys/{public_id}", silent_status_codes={404})
            client.close()
        except Exception as e:
            console.print(f"[yellow]Warning: could not reach server to revoke key ({e}). Removing local credentials anyway.[/yellow]")

    delete_credentials()
    console.print("[green]Logged out.[/green]")
