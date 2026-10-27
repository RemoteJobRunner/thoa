"""Transient network failures must not crash the CLI while it follows a job.

After the dataset stage the job runs on the backend without us, so a read
timeout or dropped connection should show a reconnect spinner and carry on,
and only give up (with instructions) after a long outage.
"""
import asyncio
import socket
import ssl
import threading
from itertools import count
from unittest.mock import MagicMock, patch

import httpx
import pytest
import typer
from rich.console import Console
from typer.testing import CliRunner
from websockets.exceptions import ConnectionClosedError

import thoa.entrypoint as ep
from thoa.cli import app
from thoa.core.api_utils import ApiClient, StreamOutcome
from thoa.core.reconnect import (
    RECONNECT_MESSAGE,
    JobReconnector,
    TransientServerError,
    is_transient,
)

runner = CliRunner()


def _console():
    return Console(force_terminal=False, width=200, record=True)


def _reconnector(console=None, give_up_after=600, retry_interval=5):
    clock = count(0.0, retry_interval)
    return JobReconnector(
        "job-1",
        console or _console(),
        give_up_after=give_up_after,
        retry_interval=retry_interval,
        sleep=lambda _: None,
        clock=lambda: next(clock),
    )


# --- what counts as transient ------------------------------------------------

_request = httpx.Request("GET", "http://example.invalid")


@pytest.mark.parametrize("exc", [
    httpx.ReadTimeout("timed out", request=_request),
    httpx.ConnectTimeout("timed out", request=_request),
    httpx.WriteTimeout("timed out", request=_request),
    httpx.PoolTimeout("timed out", request=_request),
    httpx.ConnectError("refused", request=_request),
    httpx.ReadError("reset", request=_request),
    httpx.WriteError("broken", request=_request),
    httpx.CloseError("close", request=_request),
    httpx.RemoteProtocolError("server disconnected", request=_request),
    httpx.ProxyError("proxy", request=_request),
    ConnectionResetError(),
    ConnectionRefusedError(),
    ConnectionAbortedError(),
    BrokenPipeError(),
    TimeoutError(),
    asyncio.TimeoutError(),
    ssl.SSLError(),
    ssl.SSLEOFError(),
    socket.gaierror(),
    ConnectionClosedError(None, None),
    TransientServerError(503),
], ids=lambda e: type(e).__name__)
def test_network_errors_are_transient(exc):
    assert is_transient(exc)


@pytest.mark.parametrize("exc", [
    FileNotFoundError(), PermissionError(), ValueError(), KeyError("x"), typer.Exit(1),
], ids=lambda e: type(e).__name__)
def test_other_errors_are_not_transient(exc):
    assert not is_transient(exc)


# --- JobReconnector ----------------------------------------------------------

def test_call_retries_until_the_connection_comes_back():
    attempts = iter([httpx.ReadTimeout("t"), httpx.ConnectError("c"), "ok"])

    def flaky():
        item = next(attempts)
        if isinstance(item, Exception):
            raise item
        return item

    assert _reconnector().call(flaky) == "ok"


def test_reconnect_relabels_the_active_spinner_and_restores_it():
    reconnector = _reconnector()
    seen = []
    calls = iter([httpx.ReadTimeout("t"), "ok"])

    def flaky():
        item = next(calls)
        if isinstance(item, Exception):
            raise item
        seen.append(status.status)
        return item

    with reconnector.status("Staging your files") as status:
        reconnector.call(flaky)
        assert seen == [RECONNECT_MESSAGE]
        assert status.status == "Staging your files"


def test_gives_up_with_ui_link_and_commands():
    console = _console()
    reconnector = _reconnector(console, give_up_after=600, retry_interval=5)

    def always_down():
        raise httpx.ConnectError("no route to host")

    with pytest.raises(typer.Exit) as excinfo:
        reconnector.call(always_down)

    assert excinfo.value.exit_code == 1
    out = console.export_text()
    assert "10 minutes" in out
    assert "/workbench/jobs/job-1" in out
    assert "thoa jobs attach job-1" in out
    assert "thoa jobs cancel job-1" in out


def test_outage_window_is_ten_minutes_of_retrying():
    clock = count(0.0, 5.0)
    reconnector = JobReconnector("job-1", _console(), sleep=lambda _: None, clock=lambda: next(clock))
    fn = MagicMock(side_effect=httpx.ReadTimeout("t"))

    with pytest.raises(typer.Exit):
        reconnector.call(fn)

    # One initial try, then one per 5 s retry interval for 600 s.
    assert 100 <= fn.call_count <= 125


def test_non_transient_errors_propagate_immediately():
    fn = MagicMock(side_effect=ValueError("bug"))
    with pytest.raises(ValueError):
        _reconnector().call(fn)
    assert fn.call_count == 1


def test_background_threads_are_not_retried():
    fn = MagicMock(side_effect=httpx.ReadTimeout("t"))
    errors = []

    def worker():
        try:
            _reconnector().call(fn)
        except Exception as exc:
            errors.append(exc)

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert fn.call_count == 1
    assert isinstance(errors[0], httpx.ReadTimeout)


# --- ApiClient integration ---------------------------------------------------

def _api(handler):
    client = ApiClient(base_url="http://example.invalid", api_key="dummy")
    client.client = httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(handler))
    return client


def _flaky_handler(failures):
    failures = list(failures)

    def handler(request):
        if failures:
            failure = failures.pop(0)
            if isinstance(failure, int):
                return httpx.Response(failure, text="<html>bad gateway</html>")
            raise failure
        return httpx.Response(200, json={"ok": True})
    return handler


def test_api_client_retries_timeouts_and_gateway_errors_while_following_a_job():
    api = _api(_flaky_handler([httpx.ReadTimeout("t"), 502, 503, httpx.RemoteProtocolError("dc"), 504]))
    api.reconnector = _reconnector()
    assert api.get("/jobs") == {"ok": True}


def test_api_client_is_unchanged_without_a_reconnector(capsys):
    api = _api(_flaky_handler([httpx.ReadTimeout("t")]))
    with pytest.raises(httpx.ReadTimeout):
        api.get("/jobs")

    api = _api(_flaky_handler([503]))
    assert api.get("/jobs") is None
    assert "503" in capsys.readouterr().out


# --- thoa run / thoa jobs attach end to end ----------------------------------

def _run_backend(job_status_failures):
    """Fake backend for `thoa run` without inputs; the job completes at once."""
    failures = list(job_status_failures)

    def handler(request):
        path = request.url.path
        if request.method == "GET" and path == "/api/jobs" and "public_id" in request.url.params:
            if failures:
                failure = failures.pop(0)
                if isinstance(failure, int):
                    return httpx.Response(failure)
                raise failure
            return httpx.Response(200, json=[{"public_id": "job-1", "status": "completed"}])
        routes = {
            ("GET", "/api/users/validate_job_request"): {"valid": True},
            ("POST", "/api/environments"): {"public_id": "env-1"},
            ("GET", "/api/environments/env-1/validate"): {"env_status": "validated"},
            ("POST", "/api/scripts"): {"public_id": "script-1"},
            ("POST", "/api/jobs"): {"public_id": "job-1"},
            ("PUT", "/api/jobs/job-1"): {"public_id": "job-1"},
            ("GET", "/api/jobs/job-1/attempts"): [
                {"public_id": "attempt-1", "attempt_number": 1, "status": "completed"}
            ],
        }
        return httpx.Response(200, json=routes.get((request.method, path), {}))

    return handler


def _invoke_run(api):
    run_time = MagicMock()
    run_time.time.side_effect = count(0.0, 5.0)
    reconnect_time = MagicMock()
    reconnect_time.monotonic.side_effect = count(0.0, 5.0)
    api.stream_logs_blocking = MagicMock(return_value=StreamOutcome.SUCCEEDED)

    with patch("thoa.cli.commands.run.api_client", api), \
         patch("thoa.core.job_utils.api_client", api), \
         patch("thoa.cli.commands.run.time", run_time), \
         patch("thoa.core.reconnect.time", reconnect_time), \
         patch("thoa.core.resolve_environment_spec", return_value=""):
        return runner.invoke(app, ["run", "--tools", "bash", "--cmd", "echo hi"])


def test_run_survives_brief_connection_loss_after_dataset_stage():
    api = _api(_run_backend([
        httpx.ReadTimeout("t"), httpx.ConnectError("c"), 503, httpx.RemoteProtocolError("dc"),
    ]))

    result = _invoke_run(api)

    assert result.exit_code == 0, result.output
    assert result.exception is None, repr(result.exception)
    assert api.reconnector is None, "reconnect mode must not outlive the command"


def test_run_gives_up_after_long_outage_with_instructions():
    api = _api(_run_backend([httpx.ConnectError("down")] * 1000))

    result = _invoke_run(api)

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert "thoa jobs attach job-1" in result.output
    assert "thoa jobs cancel job-1" in result.output
    assert "/workbench/jobs/job-1" in result.output
    assert api.reconnector is None


def test_attach_survives_brief_connection_loss():
    statuses = iter([httpx.ReadTimeout("t"), "provisioning", httpx.ConnectError("c"), "running"])

    def handler(request):
        item = next(statuses)
        if isinstance(item, Exception):
            raise item
        return httpx.Response(200, json=[{"public_id": "job-1", "status": item}])

    api = _api(handler)
    api.stream_logs_blocking = MagicMock(return_value=StreamOutcome.SUCCEEDED)
    reconnect_time = MagicMock()
    reconnect_time.monotonic.side_effect = count(0.0, 5.0)

    with patch("thoa.cli.commands.jobs.api_client", api), \
         patch("thoa.core.job_utils.api_client", api), \
         patch("thoa.cli.commands.jobs.time"), \
         patch("thoa.core.reconnect.time", reconnect_time):
        result = runner.invoke(app, ["jobs", "attach", "job-1"])

    assert result.exit_code == 0, result.output
    api.stream_logs_blocking.assert_called_once()
    assert api.reconnector is None


# --- everywhere else: a clean message, not a traceback -----------------------

def test_entrypoint_reports_network_errors_without_traceback(monkeypatch, capsys):
    monkeypatch.setattr(ep, "block_windows_unless_wsl", lambda: None)
    monkeypatch.setattr(ep, "check_min_client_version", lambda base_url: None)
    monkeypatch.setattr(ep, "check_maintenance_notice", lambda *a: None)

    def broken_app():
        raise httpx.ConnectError("Name or service not known")

    monkeypatch.setattr(ep, "app", broken_app)

    with pytest.raises(SystemExit) as excinfo:
        ep.main()

    assert excinfo.value.code == 1
    err = capsys.readouterr().err
    assert "Could not reach THOA" in err
    assert "Traceback" not in err


def test_entrypoint_does_not_hide_local_errors(monkeypatch):
    monkeypatch.setattr(ep, "block_windows_unless_wsl", lambda: None)
    monkeypatch.setattr(ep, "check_min_client_version", lambda base_url: None)
    monkeypatch.setattr(ep, "check_maintenance_notice", lambda *a: None)

    def broken_app():
        raise FileNotFoundError("input.txt")

    monkeypatch.setattr(ep, "app", broken_app)

    with pytest.raises(FileNotFoundError):
        ep.main()
