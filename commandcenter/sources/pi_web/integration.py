import asyncio
import functools
import logging
import math
from collections.abc import Awaitable, Iterable
from contextvars import Context
from typing import Callable, Dict, List, Set

from aiohttp import ClientSession
from hyprxa.timeseries import BaseIntegration, IntegrationClosed

from commandcenter.sources.pi_web.connection import PIWebConnection
from commandcenter.sources.pi_web.models import PIWebSubscription, WebIdType



_LOGGER = logging.getLogger("commandcenter.sources.pi_web")


class PIWebIntegration(BaseIntegration):
    """Integration implementation for real-time streaming from the PI Web API."""
    def __init__(
        self,
        session: ClientSession,
        web_id_type: WebIdType = WebIdType.FULL,
        max_connections: int = 100,
        max_subscriptions: int = 30,
        max_buffered_messages: int = 1000,
        max_reconnect_attempts: int = 5,
        initial_backoff: float = 5,
        max_backoff: float = 60,
        protocols: Iterable[str] | None = None,
        heartbeat: float = 20,
        close_timeout: float = 10,
        max_msg_size: int = 4*1024*1024
    ) -> None:
        super().__init__(max_buffered_messages)
        
        self._start_connection: Callable[
            [PIWebConnection, Set[PIWebSubscription], asyncio.Queue],
            Awaitable[None]
        ]= functools.partial(
            PIWebConnection.start,
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

        self._session = session
        self._max_capacity = max_connections * max_subscriptions
        self._max_subscriptions = max_subscriptions
        
        self._consolidation: asyncio.Task = Context().run(
            asyncio.create_task,
            self._run_consolidation()
        )
        self._event: asyncio.Event = asyncio.Event()
        self._lock: asyncio.Lock = asyncio.Lock()
    
    @property
    def capacity(self) -> int:
        return self._max_capacity - len(self.subscriptions)

    @property
    def closed(self) -> bool:
        return self._session.closed

    async def close(self) -> None:
        fut, self._consolidation = self._consolidation, None
        if fut is not None:
            fut.cancel()
        for fut, _ in self._connections.items(): fut.cancel()
        if not self._session.closed:
            await self._session.close()

    async def subscribe(self, subscriptions: Set[PIWebSubscription]) -> bool:
        if self.closed:
            raise IntegrationClosed()

        async with self._lock:
            subscriptions = list(subscriptions.difference(self.subscriptions))
            capacity = self.capacity
            count = len(subscriptions)
        
            if subscriptions and capacity >= count:
                connections = await self._create_connections(subscriptions)
                if connections is None:
                    return False
                for fut, connection in connections.items():
                    self.add_connection(fut, connection)
                    connection.toggle()
                self._event.set()
                _LOGGER.debug("Subscribed to %i subscriptions", count)
            elif subscriptions and capacity < count:
                return False
            return True

    async def unsubscribe(self, subscriptions: Set[PIWebSubscription]) -> bool:
        if self.closed:
            raise IntegrationClosed()

        async with self._lock:
            not_applicable = subscriptions.difference(self.subscriptions)
            subscriptions = subscriptions.difference(not_applicable)
        
            if subscriptions:
                # Determine the subscriptions we need to keep from existing
                # connections
                to_keep: List[PIWebSubscription] = []
                to_cancel: Dict[asyncio.Task, PIWebConnection] = {}
                for fut, connection in self._connections.items():
                    if (
                        len(connection.subscriptions.difference(subscriptions)) !=
                        len(connection.subscriptions)
                    ):
                        to_cancel[fut] = connection
                        to_keep.extend(connection.subscriptions.difference(subscriptions))
                
                if to_keep:
                    connections = await self._create_connections(to_keep)
                    if connections is None:
                        return False
                    for fut, connection in connections.items():
                        self.add_connection(fut, connection)
                        connection.toggle()
                
                # We only close the other connections if there were no connections
                # to keep or the other connections started up correctly
                for fut, connection in to_cancel.items():
                    connection.toggle()
                    fut.cancel()
                
                self._event.set()
                _LOGGER.debug("Unsubscribed from %i subscriptions", len(subscriptions))
            return True

    async def _create_connections(
        self,
        subscriptions: List[str]
    ) -> Dict[asyncio.Task, PIWebConnection] | None:
        """Creates the optimal number of connections to support all
        subscriptions.
        """
        connections: List[PIWebConnection] = []
        starters: List[Awaitable[asyncio.Future]] = []
        while True:
            if len(subscriptions) <= self._max_subscriptions:
                # This connection will cover the remaining
                connection = PIWebConnection(max_subscriptions=self._max_subscriptions)
                starters.append(
                    self._start_connection(
                        connection,
                        set(subscriptions),
                        self._data
                    )
                )
                connections.append(connection)
                break
            
            else:
                # More connections are required
                connection = PIWebConnection(max_subscriptions=self._max_subscriptions)
                starters.append(
                    self._start_connection(
                        connection,
                        set(subscriptions[:self._max_subscriptions]),
                        self._data
                    )
                )
                connections.append(connection)
                del subscriptions[:self._max_subscriptions]

        results = await asyncio.gather(*starters, return_exceptions=True)
        if any([isinstance(result, Exception) for result in results]):
            for result in results:
                if isinstance(result, asyncio.Future):
                    result.cancel()
                elif isinstance(result, Exception):
                    _LOGGER.warning("Connection failed to start", exc_info=result)
            return None

        for fut in results:
            fut.add_done_callback(self.connection_lost)
        
        return {fut: connection for fut, connection in zip(results, connections)}

    async def _run_consolidation(self) -> None:
        """Ensures the optimal number of websocket connections are open.
        
        `subscribe` is a purely additive operation while `unsubscribe` can leave
        connections without an optimal number of connections. This class closes
        connections and reopens them with the maximum number of subscriptions
        to optimize connection usage.
        """
        while True:
            await self._event.wait()
            self._event.clear()
            await asyncio.sleep(5)

            async with self._lock:
                _LOGGER.debug("Scanning connections for consolidation")
                has_capacity = {
                    fut: connection for fut, connection in self._connections.items()
                    if len(connection.subscriptions) < self._max_subscriptions
                }
                capacity = sum(
                    [
                        len(connection.subscriptions) for connection
                        in has_capacity.values()
                    ]
                )
                optimal = math.ceil(capacity/self._max_subscriptions)
                
                if optimal == len(has_capacity):
                    _LOGGER.debug("Optimal number of active connection")
                    continue

                _LOGGER.debug("Consolidating connections")
                
                subscriptions = []
                for connection in has_capacity.values():
                    subscriptions.extend(connection.subscriptions)
                
                connections = await self._create_connections(subscriptions)
                if connections is None:
                    _LOGGER.info("Consolidation failed, unable to start up supplemental connections")
                    continue
                for fut, connection in connections.items():
                    self.add_connection(fut, connection)
                    connection.toggle()

                for fut, connection in has_capacity.items():
                    connection.toggle()
                    fut.cancel()
                
                _LOGGER.debug(
                    "Consolidated %i connections",
                    len(has_capacity)-len(connections)
                )