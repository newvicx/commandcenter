import asyncio
import logging
from collections.abc import Iterable
from contextvars import Context
from typing import Dict, Set

from aiohttp import ClientSession, ClientWebSocketResponse
from hyprxa.timeseries import BaseConnection
from hyprxa.util import EqualJitterBackoff
from pydantic import ValidationError

from commandcenter.sources.pi_web.models import (
    PIWebMessage,
    PIWebSubscription,
    PIWebSubscriptionMessage,
    WebIdType
)



_LOGGER = logging.getLogger("commandcenter.sources.pi_web")



def build_url(web_id_type: WebIdType, subscriptions: Set[PIWebSubscription]) -> str:
    """Build websocket '/streamset' url."""
    for subscription in subscriptions:
        if subscription.web_id_type != web_id_type:
            raise ValueError(f"Invalid WebIdType for subscription {subscription.web_id}")
    
    return (
        "/piwebapi/streamsets/channel?webId="
        f"{'&webId='.join([subscription.web_id for subscription in subscriptions])}"
        f"&webIdType={web_id_type.value}&includeInitialValues=true"
    )


async def create_connection(
    session: ClientSession,
    url: str,
    protocols: Iterable[str] | None,
    heartbeat: float,
    close_timeout: float,
    max_msg_size: int,
) -> ClientWebSocketResponse:
    """Open websocket connection to PI Web API."""
    return await session.ws_connect(
        url,
        protocols=protocols,
        heartbeat=heartbeat,
        timeout=close_timeout,
        max_msg_size=max_msg_size
    )


class PIWebConnection(BaseConnection):
    """Represents a single websocket connection to a PI Web API '/streamset'
    endpoint.
    """
    def __init__(self, max_subscriptions: int) -> None:
        super().__init__()
        self._max_subscriptions = max_subscriptions
        self._index: Dict[str, PIWebSubscription] = {}

    @property
    def capacity(self) -> int:
        """The number of additional subscriptions this connection can support."""
        return self._max_subscriptions - len(self._subscriptions)

    def build_index(self) -> None:
        """Build the WebId - Subscription index."""
        self._index = {subscription.web_id: subscription for subscription in self.subscriptions}

    async def run(
        self,
        session: ClientSession,
        web_id_type: WebIdType,
        max_reconnect_attempts: int,
        initial_backoff: float,
        max_backoff: float,
        protocols: Iterable[str] | None,
        heartbeat: float,
        close_timeout: float,
        max_msg_size: int
    ) -> None:
        """Open a websocket connection to the PI Web API and process data
        indefinitely.
        """
        self.build_index()
        url = build_url(web_id_type=web_id_type, subscriptions=self._subscriptions)
        ws = await create_connection(
            session=session,
            url=url,
            protocols=protocols,
            heartbeat=heartbeat,
            close_timeout=close_timeout,
            max_msg_size=max_msg_size
        )
        backoff = EqualJitterBackoff(max_backoff, initial_backoff)
        _LOGGER.debug(
            "Established connection for %i subscriptions",
            len(self._subscriptions),
            extra={"url": url}
        )
        self._started.set()

        try:
            while True:
                async for msg in ws:
                    try:
                        data = PIWebMessage.parse_raw(msg.data)
                    except ValidationError:
                        _LOGGER.warning(
                            "Message validation failed",
                            exc_info=True,
                            extra={"raw": msg.data}
                        )
                    except Exception:
                        _LOGGER.error(
                            "An unhandled error occurred parsing the message",
                            exc_info=True,
                            extra={"raw": msg.data}
                        )
                    else:
                        for item in data.items:
                            web_id = item.web_id
                            assert web_id in self._index
                            subscription = self._index[web_id]
                            message = PIWebSubscriptionMessage(
                                subscription=subscription,
                                items=[subitem.dict() for subitem in item.items]
                            )
                            await self._data.put(message)
                            self._total_published += 1
                            _LOGGER.debug("Published message on %s", self.__class__.__name__)
                else:
                    assert ws.closed
                    close_code = ws.close_code
                    _LOGGER.warning(
                        "Websocket closed by peer or network failure %i",
                        close_code
                    )
                    e = ws.exception()
                    if max_reconnect_attempts is not None and max_reconnect_attempts > 0:
                        while backoff.failures < max_reconnect_attempts:
                            _LOGGER.info(
                                "Attempting reconnect. Attempt %i of %i",
                                backoff.failures + 1,
                                max_reconnect_attempts
                            )
                            try:
                                ws = await create_connection(
                                    session=session,
                                    url=url,
                                    protocols=protocols,
                                    heartbeat=heartbeat,
                                    close_timeout=close_timeout,
                                    max_msg_size=max_msg_size
                                )
                            except Exception:
                                backoff_delay = backoff.compute()
                                _LOGGER.debug(
                                    "Reconnect failed. Trying again in %0.2f seconds",
                                    backoff_delay,
                                    exc_info=True
                                )
                                await asyncio.sleep(backoff_delay)
                            else:
                                _LOGGER.info("Connection re-established")
                                backoff.reset()
                                break
                        if ws.closed:
                            if e is not None:
                                raise e
                            break
                    else:
                        if e is not None:
                            raise e
                        break
        finally:
            self._started.clear()
            if not ws.closed:
                await ws.close()
    
    async def start(
        self,
        subscriptions: Set[PIWebSubscription],
        data: asyncio.Queue,
        session: ClientSession,
        web_id_type: WebIdType,
        max_reconnect_attempts: int,
        initial_backoff: float,
        max_backoff: float,
        protocols: Iterable[str] | None,
        heartbeat: float,
        close_timeout: float,
        max_msg_size: int
    ) -> asyncio.Future:
        self._subscriptions.update(subscriptions)
        self._data = data
        runner = Context().run(
            asyncio.create_task,
            self.run(
                session=session,
                web_id_type=web_id_type,
                max_reconnect_attempts=max_reconnect_attempts,
                initial_backoff=initial_backoff,
                max_backoff=max_backoff,
                protocols=protocols,
                heartbeat=heartbeat,
                close_timeout=close_timeout,
                max_msg_size=max_msg_size
            )
        )
        waiter = asyncio.create_task(self._started.wait())
        try:
            await asyncio.wait([runner, waiter], return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            runner.cancel()
            waiter.cancel()
            raise
        if not waiter.done():
            e = runner.exception()
            if e is not None:
                raise e
            raise RuntimeError("Runner exited without throwing exception.")
        return runner