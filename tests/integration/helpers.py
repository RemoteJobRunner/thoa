import time
import httpx
import os


def api_url() -> str:
    return os.environ.get("THOA_STAGING_API_URL", os.environ.get("THOA_API_URL", ""))


def api_key() -> str:
    return os.environ.get("THOA_STAGING_API_KEY", os.environ.get("THOA_API_KEY", ""))


def api_headers() -> dict:
    return {"X-API-Key": api_key(), "Accept": "application/json"}


def api_get(path: str, **kwargs) -> httpx.Response:
    return httpx.get(f"{api_url()}/api{path}", headers=api_headers(), timeout=30, **kwargs)


def api_post(path: str, **kwargs) -> httpx.Response:
    return httpx.post(f"{api_url()}/api{path}", headers=api_headers(), timeout=30, **kwargs)


def api_put(path: str, **kwargs) -> httpx.Response:
    return httpx.put(f"{api_url()}/api{path}", headers=api_headers(), timeout=30, **kwargs)


def create_file(filename: str = "test.txt", size: int = 100) -> str:
    resp = api_post("/files", json={
        "filename": filename,
        "size": size,
        "md5sum": "d41d8cd98f00b204e9800998ecf8427e",
    })
    assert resp.status_code == 200, resp.text
    return resp.json()["public_id"]


def create_dataset(file_ids: list[str]) -> dict:
    resp = api_post("/datasets", json={"files": file_ids})
    assert resp.status_code == 200, resp.text
    return resp.json()


def get_job_status(job_public_id: str) -> str:
    resp = api_get("/jobs", params={"public_id": job_public_id})
    if resp.status_code != 200 or not resp.json():
        return "unknown"
    return resp.json()[0].get("status", "unknown")


def poll_job_until_terminal(job_public_id: str, timeout: int = 600, interval: int = 10) -> str:
    """Poll job status until it reaches a terminal state or times out.
    Returns the final status string.
    Terminal states: completed, failed, cancelled, error
    """
    terminal = {"completed", "failed", "cancelled", "error"}
    deadline = time.time() + timeout
    status = "unknown"
    while time.time() < deadline:
        status = get_job_status(job_public_id)
        if status in terminal:
            return status
        time.sleep(interval)
    return status
