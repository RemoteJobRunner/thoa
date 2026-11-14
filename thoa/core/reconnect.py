"""Riding out transient network failures while following a job.

Once a job is past its dataset stage it runs on the backend whether or not
the CLI is connected, so a dropped connection is no reason to crash. While a
``JobReconnector`` is active, API requests that fail with a transient error
are retried behind a "Connection lost" spinner until the backend answers
again, or until ``give_up_after`` seconds of continuous failure, at which
point the user is told how to find, re-attach to, or cancel the job.
"""
import asyncio
import contextlib
import socket
import ssl
import threading
import time
from typing import Callable, Optional, TypeVar

import httpx
import typer
import websockets.exceptions
from rich.console import Console
from rich.errors import LiveError
from rich.panel import Panel

from thoa.config import settings

T = TypeVar("T")

# Everything that means "we couldn't talk to the server right now" rather
# than "the server told us something". httpx.TransportError covers every
# timeout (connect/read/write/pool), connection resets and refusals, remote
# protocol errors (e.g. server dropped the connection mid-response) and proxy
# errors. The socket-level errors below catch failures that escape httpx's
# mapping (and websockets, which raises them directly). Plain OSError is
# deliberately excluded so local file errors are never mistaken for a lost
# connection. asyncio.TimeoutError is distinct from TimeoutError on 3.10.
TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    httpx.TransportError,
    websockets.exceptions.WebSocketException,
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
    ssl.SSLError,
    socket.gaierror,
    socket.herror,
)

# Gateway/load-balancer responses seen during deploys and brief outages.
TRANSIENT_STATUS_CODES = frozenset({502, 503, 504})

RECONNECT_MESSAGE = "Connection lost, trying to reconnect to your job..."
DEFAULT_GIVE_UP_AFTER = 600  # seconds
DEFAULT_RETRY_INTERVAL = 5  # seconds


class TransientServerError(Exception):
    """A gateway status (502/503/504) we should retry rather than report."""

    def __init__(self, status_code: int):
        super().__init__(f"server returned {status_code}")
        self.status_code = status_code


def is_transient(exc: BaseException) -> bool:
    return isinstance(exc, TRANSIENT_ERRORS + (TransientServerError,))


class JobReconnector:
    def __init__(
        self,
        job_id: str,
        console: Console,
        *,
        give_up_after: float = DEFAULT_GIVE_UP_AFTER,
        retry_interval: float = DEFAULT_RETRY_INTERVAL,
        sleep: Optional[Callable[[float], None]] = None,
        clock: Optional[Callable[[], float]] = None,
    ):
        self.job_id = job_id
        self.console = console
        self.give_up_after = give_up_after
        self.retry_interval = retry_interval
        self._sleep = sleep or time.sleep
        self._clock = clock or time.monotonic
        self._active_status = None
        self._active_message: Optional[str] = None

    @contextlib.contextmanager
    def status(self, message: str, spinner: str = "dots12"):
        """``console.status`` that the reconnect spinner can take over.

        rich only allows one live display at a time, so instead of opening a
        second spinner during a reconnect we re-label the one already shown.
        """
        with self.console.status(message, spinner=spinner) as status:
            previous = (self._active_status, self._active_message)
            self._active_status, self._active_message = status, message
            try:
                yield status
            finally:
                self._active_status, self._active_message = previous

    def call(self, fn: Callable[[], T]) -> T:
        """Run ``fn``, retrying transient failures until it succeeds or we give up.

        Only the main thread retries: background pollers keep their own error
        handling and must never block or exit the CLI.
        """
        if threading.current_thread() is not threading.main_thread():
            return fn()

        try:
            return fn()
        except BaseException as exc:
            if not is_transient(exc):
                raise
            last_error: BaseException = exc

        deadline = self._clock() + self.give_up_after
        with self._reconnecting_display():
            while True:
                if self._clock() >= deadline:
                    self._give_up(last_error)
                self._sleep(self.retry_interval)
                try:
                    result = fn()
                except BaseException as exc:
                    if not is_transient(exc):
                        raise
                    last_error = exc
                    continue
                return result

    @contextlib.contextmanager
    def _reconnecting_display(self):
        if self._active_status is not None:
            self._active_status.update(RECONNECT_MESSAGE)
            try:
                yield
            finally:
                self._active_status.update(self._active_message)
            return

        try:
            own_status = self.console.status(RECONNECT_MESSAGE, spinner="dots12")
            own_status.start()
        except LiveError:
            # Some other live display (e.g. a progress bar) owns the terminal.
            self.console.print(f"[yellow]{RECONNECT_MESSAGE}[/yellow]")
            yield
            return

        try:
            yield
        finally:
            own_status.stop()

    def _give_up(self, last_error: BaseException) -> None:
        minutes = self.give_up_after / 60
        duration = f"{minutes:g} minute{'s' if minutes != 1 else ''}"
        self.console.print(Panel(
            f"[bold red]Could not reconnect to THOA after {duration}.[/bold red]\n"
            f"[dim]Last error: {describe_error(last_error)}[/dim]\n\n"
            "Your job keeps running on THOA.\n\n"
            f"[bold]View in the UI:[/bold]  {settings.THOA_UI_URL}/workbench/jobs/{self.job_id}\n"
            f"[bold]Reconnect:[/bold]       thoa jobs attach {self.job_id}\n"
            f"[bold]Cancel:[/bold]          thoa jobs cancel {self.job_id}",
            title="Connection lost",
            expand=False,
            border_style="red",
        ))
        raise typer.Exit(code=1)


def describe_error(exc: BaseException) -> str:
    text = str(exc).strip()
    return f"{type(exc).__name__}: {text}" if text else type(exc).__name__
