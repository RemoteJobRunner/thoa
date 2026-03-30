"""
conftest.py for CLI integration tests.

Spins up:
  1. Test Postgres via docker-compose.test.yml (port 5454)
  2. Runs setup_test_db.py via backend's venv python to create schema + test user + API key
  3. Starts backend process with .env.test on port 9998
  4. Sets THOA_API_URL / THOA_API_KEY env vars for CLI to use
  5. Monkey-patches module-level api_client singletons to point at the test backend
     (fix /api/ prefix that the test backend does not have; the _DirectApiClient subclass skips that prefix)
"""

import os
import time
import subprocess
import pytest
import httpx

BACKEND_DIR = os.path.expanduser("~/app/backend")
BACKEND_PYTHON = os.path.join(BACKEND_DIR, "venv", "bin", "python")
SETUP_SCRIPT = os.path.join(os.path.dirname(__file__), "setup_test_db.py")
BACKEND_TEST_PORT = 9998
BACKEND_TEST_URL = f"http://localhost:{BACKEND_TEST_PORT}"


def _load_env_test() -> dict:
    env_path = os.path.join(BACKEND_DIR, ".env.test")
    env = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def _start_test_db():
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.test.yml", "up", "-d", "--wait"],
        cwd=BACKEND_DIR,
        capture_output=True,
    )
    time.sleep(2)


def _setup_db_and_get_key(env: dict) -> str:
    """Run setup_test_db.py in backend's venv, capture the printed API key."""
    proc_env = {**env, "PYTHONPATH": BACKEND_DIR}
    result = subprocess.run(
        [BACKEND_PYTHON, SETUP_SCRIPT],
        cwd=BACKEND_DIR,
        env=proc_env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"setup_test_db.py failed:\n{result.stderr}")
    return result.stdout.strip()


def _start_backend(env: dict) -> subprocess.Popen:
    proc_env = {**os.environ, **env, "PYTHONPATH": BACKEND_DIR}
    return subprocess.Popen(
        [
            BACKEND_PYTHON, "-m", "uvicorn",
            "server:app",
            "--host", "0.0.0.0",
            "--port", str(BACKEND_TEST_PORT),
        ],
        cwd=BACKEND_DIR,
        env=proc_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _wait_for_backend(url: str, timeout: int = 30) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(f"{url}/docs", timeout=2)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def _patch_api_client(base_url: str, api_key: str) -> None:
    """
    Replace the module-level api_client singletons used by CLI commands with a
    direct test client.

    The dev/production ApiClient._request prepends /api to every path (because
    dev/production sits behind an nginx proxy that strips /api). The test backend
    exposes routes without that prefix, so we need a subclass that skips it.
    """
    from thoa.core.api_utils import ApiClient
    import thoa.core.dataset_utils as _ds
    import thoa.core.job_utils as _jobs
    import thoa.cli.commands.run as _run

    class _DirectApiClient(ApiClient):
        def _request(self, method: str, path: str, **kwargs):
            if not self.api_key:
                return None
            response = self.client.request(method, path, **kwargs)
            if response.status_code == 200:
                return response.json()
            return None

    test_client = _DirectApiClient(base_url=base_url, api_key=api_key)
    _ds.client = test_client
    _jobs.api_client = test_client
    _run.api_client = test_client


@pytest.fixture(scope="session")
def backend_url_and_key():
    _start_test_db()

    env = _load_env_test()

    private_key = _setup_db_and_get_key(env)

    backend_proc = _start_backend(env)

    if not _wait_for_backend(BACKEND_TEST_URL, timeout=30):
        backend_proc.terminate()
        pytest.fail("Backend did not start in time")

    os.environ["THOA_API_URL"] = BACKEND_TEST_URL
    os.environ["THOA_API_KEY"] = private_key

    _patch_api_client(BACKEND_TEST_URL, private_key)

    yield BACKEND_TEST_URL, private_key

    backend_proc.terminate()
    backend_proc.wait()
