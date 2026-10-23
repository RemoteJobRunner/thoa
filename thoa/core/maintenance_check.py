import sys
from typing import Optional

import httpx

from thoa.core.version_check import current_version


_TIMEOUT_SECONDS = 1.5


def check_maintenance_notice(base_url: str, api_key: Optional[str]) -> None:
    """
    Best-effort startup probe. Prints the current maintenance notice
    (announcement or ongoing maintenance) to stderr. Never raises.
    """
    if not api_key:
        return
    try:
        resp = httpx.get(
            base_url.rstrip("/") + "/api/maintenance",
            headers={"X-API-Key": api_key, "X-Client-Version": current_version()},
            timeout=_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            return
        body = resp.json()
        if not isinstance(body, dict) or not body.get("active") or not body.get("message"):
            return
        print(f"\033[33m{body['message']}\033[0m", file=sys.stderr)
    except (httpx.HTTPError, ValueError):
        return
