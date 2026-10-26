from thoa.cli import app
from thoa.core.env_utils import block_windows_unless_wsl
from thoa.core.version_check import check_min_client_version
from thoa.core.maintenance_check import check_maintenance_notice
from thoa.config import settings, get_api_key


def main():
    block_windows_unless_wsl()
    check_min_client_version(settings.THOA_API_URL)
    check_maintenance_notice(settings.THOA_API_URL, get_api_key())
    app()
