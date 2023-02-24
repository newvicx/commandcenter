import csv
import io
import logging
from collections.abc import Awaitable, Iterable
from typing import Tuple

from aiohttp import ClientResponse, ClientResponseError
from hyprxa.types import JSONPrimitive

from commandcenter.sources.traxx.exceptions import TraxxExpiredSession



_LOGGER = logging.getLogger("commandcenter.sources.traxx")


async def handle_request(
    coro: Awaitable[ClientResponse],
    raise_for_status: bool = True
) -> Iterable[Tuple[str, JSONPrimitive]] | None:
    """Primary response handling for all HTTP requests to Traxx."""
    response = await coro
    
    try:
        response.raise_for_status()
    except ClientResponseError as err:
        await response.release()
        if raise_for_status:
            raise
        _LOGGER.warning("Error in client response (%i)", err.code, exc_info=True)
        return None
    
    async with response as ctx:
        content = await ctx.read()
    
    if not content:
        return None
    elif b"<!DOCTYPE html>" in content:
        raise TraxxExpiredSession()

    buffer = io.StringIO(content.decode())
    return csv.reader(buffer)