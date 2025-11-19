from thoa.cli import app
from thoa.core.env_utils import block_windows_unless_wsl

def main():
    block_windows_unless_wsl()
    app()