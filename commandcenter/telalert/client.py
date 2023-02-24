import asyncio
import concurrent.futures
import functools
import logging
import os
import pathlib
from typing import List

import anyio
from hyprxa.util import log_subprocess

from commandcenter.telalert.models import TelAlertMessage


_LOGGER = logging.getLogger("commandcenter.dialout.client")


class TelAlertClient:
    """Client for sending alerts through the TelAlert system.

    Args:
        path: Path to the `telalert.exe` application.
        host: Target host for dial out requests.
        max_workers: The number of sub processes that can be run concurrently.
        timeout: The max time for a call to `telalert.exe` to complete.
    
    Raises:
        FileNotFoundError: The path to `telalert.exe` was not found.
    
    Examples:
    >>> client = TelAlertClient(path, "myhost")
    >>> # Send a notification to a group
    >>> await send_alert("Something happened", groups=["mygroup"])
    >>> # You can send alerts to multiple groups in one call
    >>> await send_alert("Its bad guys", groups=["mygroup", "thatgroup"])
    >>> # You can also mix and match groups and destinations
    >>> await send_alert("Dont tell him", groups=["mygroup", "thatgroup"], destinations=["CEO"])
    """
    def __init__(
        self,
        path: os.PathLike,
        host: str,
        max_workers: int = 4,
        timeout: float = 3
    ) -> None:
        path = pathlib.Path(path)
        if not path.exists():
            raise FileNotFoundError(str(path))
        self._path = path
        self._host = host
        self._timeout = timeout

        self._executor: concurrent.futures.ThreadPoolExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._lock: asyncio.Semaphore = asyncio.Semaphore(max_workers)
        self._loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()

    async def send_alert(self, message: TelAlertMessage) -> None:
        """Send a notification through the TelAlert system to any number of
        destinations or groups.
        
        Args
            message: The message to send.
        """
        commands = []
        msg = message.msg
        subject = message.subject or ""
        for group in message.groups:
            commands.append([self._path, "-g", group, "-m", msg, "-host", self._host, "-subject", subject])
        for destination in message.destinations:
            commands.append([self._path, "-i", destination, "-m", msg, "-host", self._host, "-subject", subject])
        
        tasks = [self._execute_command(command) for command in commands]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for maybe_exc, command in zip(results, commands):
            if isinstance(maybe_exc, BaseException):
                _LOGGER.error("Notification failed", exc_info=maybe_exc, extra={"command": command})
        
    async def _execute_command(self, command: List[str]) -> None:
        """Execute the command in a subprocess."""
        async with self._lock:
            await anyio.to_thread.run_sync(
                functools.partial(
                    log_subprocess,
                    timeout=self._timeout,
                    check=True
                ),
                _LOGGER,
                command
            )