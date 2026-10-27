import sys

from thoa.cli import app
from thoa.core.env_utils import block_windows_unless_wsl
from thoa.core.version_check import check_min_client_version
from thoa.core.maintenance_check import check_maintenance_notice
from thoa.core.reconnect import TRANSIENT_ERRORS, describe_error
from thoa.config import settings, get_api_key


def main():
    block_windows_unless_wsl()
    check_min_client_version(settings.THOA_API_URL)
    check_maintenance_notice(settings.THOA_API_URL, get_api_key())
    try:
        app()
    except TRANSIENT_ERRORS as exc:
        # A network blip outside a followed job (those retry on their own):
        # explain it in one line instead of dumping a traceback.
        print(
            f"\033[31mCould not reach THOA ({describe_error(exc)}).\033[0m\n"
            "Check your internet connection and try again.",
            file=sys.stderr,
        )
        sys.exit(1)
