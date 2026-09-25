"""`thoa run -i <accession>`: manifest before the job, server-side import, detach on Ctrl-C."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from thoa.cli import app
from thoa.core.local_transfer import PreparedTransfer

runner = CliRunner()

TRANSFER_ID = "transfer-public-1"
JOB_ID = "job-public-1"


def _mock_api(calls):
    def post(path, **kwargs):
        calls.append(("POST", path, kwargs.get("json")))
        if path == "/scripts":
            return {"public_id": "script-1"}
        if path == "/jobs":
            return {"public_id": JOB_ID}
        if path == "/environments":
            return {"public_id": "env-1"}
        return {}

    def put(path, **kwargs):
        calls.append(("PUT", path, kwargs.get("json")))
        return {"public_id": JOB_ID}

    def get(path, **kwargs):
        if path == "/users/validate_job_request":
            return {"valid": True}
        return {}

    api = MagicMock()
    api.post.side_effect = post
    api.put.side_effect = put
    api.get.side_effect = get
    return api


def _invoke(extra_args, *, track=None):
    calls, order = [], []
    api = _mock_api(calls)
    prepared = PreparedTransfer(transfer_id=TRANSFER_ID, manifest={"items": [{"provider": "sra", "size": 10}]})

    def prepare(specs, cwd):
        order.append(("prepare", [(s.kind, s.source) for s in specs]))
        # The job must not exist yet when the manifest is built.
        assert not any(c[1] == "/jobs" for c in calls)
        return prepared

    mock_time = MagicMock()
    mock_time.time.return_value = 0.0
    with patch("thoa.cli.commands.run.api_client", api), \
         patch("thoa.core.job_utils.api_client", api), \
         patch("thoa.cli.commands.run.prepare_mixed_transfer", side_effect=prepare), \
         patch("thoa.cli.commands.run.upload_local_items", side_effect=lambda p: order.append(("upload", p.transfer_id))), \
         patch("thoa.cli.commands.run.start_transfer", side_effect=lambda p: order.append(("start", p.transfer_id))), \
         patch("thoa.cli.commands.run.track_transfer", side_effect=track or (lambda p: {"dataset_public_id": "d", "input_context": {}})), \
         patch("thoa.cli.commands.run.current_job_status", return_value="completed"), \
         patch("thoa.cli.commands.run.time", mock_time), \
         patch("thoa.core.resolve_environment_spec", return_value=""):
        result = runner.invoke(app, ["run", "-i", "SRR390728", "--tools", "bash", "--cmd", "echo hi", *extra_args])
    return result, calls, order


def _job_post(calls):
    return next(body for method, path, body in calls if method == "POST" and path == "/jobs")


def test_accession_job_is_linked_to_the_prepared_import():
    result, calls, order = _invoke(["--run-async"])

    assert result.exit_code == 0, result.output
    assert order[0] == ("prepare", [("sra", "SRR390728")])
    assert _job_post(calls)["pending_import_transfer_public_id"] == TRANSFER_ID
    assert ("upload", TRANSFER_ID) in order and ("start", TRANSFER_ID) in order


def test_run_async_returns_right_after_starting_the_import():
    result, calls, _ = _invoke(["--run-async"])
    assert JOB_ID in result.output
    assert "runs server-side" in result.output
    # The backend attaches the dataset itself; the CLI never PUTs it.
    assert not any(method == "PUT" and body and "input_dataset_public_id" in body for method, _, body in calls)


def test_ctrl_c_while_following_detaches_without_cancelling():
    def interrupted(_prepared):
        raise KeyboardInterrupt

    result, calls, _ = _invoke([], track=interrupted)

    assert result.exit_code == 0, result.output
    assert "Detached" in result.output
    assert f"thoa jobs cancel {JOB_ID}" in result.output
    assert not any(path.endswith("/cancel") for _, path, _ in calls)


def test_failed_import_exits_nonzero_without_cancelling():
    def failed(_prepared):
        raise RuntimeError("Dataset import failed: SRR390728 was not found at ENA")

    result, calls, _ = _invoke([], track=failed)

    assert result.exit_code == 1
    assert "not found at ENA" in result.output
    assert not any(path.endswith("/cancel") for _, path, _ in calls)


def test_ambiguous_accession_fails_before_anything_is_created(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "SRR390728").mkdir()
    api = MagicMock()
    with patch("thoa.cli.commands.run.api_client", api), patch("thoa.core.job_utils.api_client", api):
        result = runner.invoke(app, ["run", "-i", "SRR390728", "--tools", "bash", "--cmd", "echo hi"])
    assert result.exit_code == 1
    assert "sra:SRR390728" in result.output
    api.post.assert_not_called()
