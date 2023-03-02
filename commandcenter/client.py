import os
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Callable, TextIO

import orjson
from httpx import QueryParams
from hyprxa.client import HyprxaAsyncClient, HyprxaClient
from hyprxa.util import Status
from pydantic import BaseModel

from commandcenter.telalert.models import DialoutRequest, TelAlertMessage


class CommandCenterClient(HyprxaClient):
    def send_alert(
        self,
        msg: str,
        idempotency_key: str | None = None,
        groups: Sequence[str] | None = None,
        destinations: Sequence[str] | None = None,
        subject: str | None = None
    ) -> Status:
        """Send a PUT request to /dialout/alert"""
        message = TelAlertMessage(
            msg=msg,
            groups=groups,
            destinations=destinations,
            subject=subject
        )
        idempotency_key=idempotency_key or uuid.uuid4().hex
        data = DialoutRequest(message=message, idempotency_key=idempotency_key).dict()
        return self._put("/dialout/alert", Status, data)

    def download_alerts(
        self,
        destination: os.PathLike | TextIO = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        timezone: str | None = None,
    ) -> None:
        """Send a GET request to /dialout/recorded.
        
        Data is in CSV format.
        """
        path = "/dialout/recorded"
        params=QueryParams(
            start_time=start_time,
            end_time=end_time,
            timezone=timezone
        )
        self._download(
            method="GET",
            path=path,
            suffix=".csv",
            accept="text/csv",
            params=params,
            destination=destination
        )

    def _put(
        self,
        path: str,
        response_model: Callable[[Mapping], Any],
        json: Any,
        params: QueryParams | None = None,
    ) -> BaseModel:
        """Handle PUT request and return the response model."""
        response = self._client.put(path, params=params, json=json)
        content = response.read()
        data = orjson.loads(content)
        return response_model(**data)


class CommandCenterAsyncClient(HyprxaAsyncClient):
    async def send_alert(
        self,
        msg: str,
        idempotency_key: str | None = None,
        groups: Sequence[str] | None = None,
        destinations: Sequence[str] | None = None,
        subject: str | None = None
    ) -> Status:
        """Send a PUT request to /dialout/alert"""
        message = TelAlertMessage(
            msg=msg,
            groups=groups,
            destinations=destinations,
            subject=subject
        )
        idempotency_key=idempotency_key or uuid.uuid4().hex
        data = DialoutRequest(message=message, idempotency_key=idempotency_key).dict()
        return await self._put("/dialout/alert", Status, data)

    async def download_alerts(
        self,
        destination: os.PathLike | TextIO = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        timezone: str | None = None,
    ) -> None:
        """Send a GET request to /dialout/recorded.
        
        Data is in CSV format.
        """
        path = "/dialout/recorded"
        params=QueryParams(
            start_time=start_time,
            end_time=end_time,
            timezone=timezone
        )
        await self._download(
            method="GET",
            path=path,
            suffix=".csv",
            accept="text/csv",
            params=params,
            destination=destination
        )

    async def _put(
        self,
        path: str,
        response_model: Callable[[Mapping], Any],
        json: Any,
        params: QueryParams | None = None,
    ) -> BaseModel:
        """Handle PUT request and return the response model."""
        response = await self._client.put(path, params=params, json=json)
        content = await response.aread()
        data = orjson.loads(content)
        return response_model(**data)