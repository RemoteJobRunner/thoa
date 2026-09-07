"""Log streaming must degrade, never kill the command.

The job keeps running when the log stream is unavailable, so `stream_logs`
reports a third outcome instead of raising or claiming the attempt failed.
"""
import asyncio
from unittest.mock import patch

import pytest
from websockets.datastructures import Headers
from websockets.exceptions import InvalidStatus
from websockets.http11 import Response

from thoa.core.api_utils import ApiClient, StreamOutcome


def _client():
    return ApiClient(base_url="http://example.invalid", api_key="dummy")


class _FakeWebSocket:
    def __init__(self, frames):
        self._frames = frames

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def __aiter__(self):
        async def gen():
            for frame in self._frames:
                yield frame
        return gen()

    async def close(self):
        pass


def test_rejected_handshake_reports_unavailable():
    rejection = InvalidStatus(Response(403, "Forbidden", Headers(), b""))

    with patch("thoa.core.api_utils.websockets.connect", side_effect=rejection):
        outcome = asyncio.run(_client().stream_logs("job-1"))

    assert outcome == StreamOutcome.UNAVAILABLE


def test_stream_error_event_reports_unavailable():
    frames = ['{"event": "error", "message": "Auth service error"}']

    with patch("thoa.core.api_utils.websockets.connect", return_value=_FakeWebSocket(frames)):
        outcome = asyncio.run(_client().stream_logs("job-1"))

    assert outcome == StreamOutcome.UNAVAILABLE


def test_done_event_reports_success_or_failure():
    with patch("thoa.core.api_utils.websockets.connect",
               return_value=_FakeWebSocket(['{"event": "done", "success": 1}'])):
        assert asyncio.run(_client().stream_logs("job-1")) == StreamOutcome.SUCCEEDED

    with patch("thoa.core.api_utils.websockets.connect",
               return_value=_FakeWebSocket(['{"event": "done", "success": 0}'])):
        assert asyncio.run(_client().stream_logs("job-1")) == StreamOutcome.FAILED
