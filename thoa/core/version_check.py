import sys
from importlib.metadata import version as _pkg_version, PackageNotFoundError

import httpx
from packaging.version import Version, InvalidVersion


_TIMEOUT_SECONDS = 1.5


def _current_version() -> str:
    try:
        return _pkg_version("thoa")
    except PackageNotFoundError:
        return "0.0.0"


def check_min_client_version(base_url: str) -> None:
    """
    Best-effort startup probe. Prints a warning to stderr if the installed
    thoa CLI is older than the backend's MIN_CLIENT_VERSION. Never raises.
    """
    try:
        url = base_url.rstrip("/") + "/api/version"
        resp = httpx.get(url, timeout=_TIMEOUT_SECONDS)
        if resp.status_code != 200:
            return
        min_str = resp.json().get("min_client_version")
        if not min_str:
            return
        current = Version(_current_version())
        minimum = Version(min_str)
        if current < minimum:
            print(
                f"\033[33mWARNING: your thoa CLI is outdated "
                f"({current} < {minimum}). Run: pip install -U thoa\033[0m",
                file=sys.stderr,
            )
    except (httpx.HTTPError, ValueError, InvalidVersion, KeyError, TypeError):
        # Best-effort only — never block the CLI.
        return
