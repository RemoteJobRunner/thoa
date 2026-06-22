import json
import os
import stat
from pathlib import Path
from typing import Optional

_THOA_DIR = Path.home() / ".thoa"
_CREDENTIALS_FILE = _THOA_DIR / "credentials"


def read_credentials() -> Optional[dict]:
    try:
        with open(_CREDENTIALS_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def write_credentials(data: dict) -> None:
    _THOA_DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    with open(_CREDENTIALS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    os.chmod(_CREDENTIALS_FILE, stat.S_IRUSR | stat.S_IWUSR)


def delete_credentials() -> None:
    try:
        _CREDENTIALS_FILE.unlink()
    except FileNotFoundError:
        pass
