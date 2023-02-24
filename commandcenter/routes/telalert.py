import logging
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from fastapi.responses import StreamingResponse
from hyprxa.auth import requires
from hyprxa.dependencies import can_read, can_write, parse_timestamp
from hyprxa.dependencies.util import get_file_writer
from hyprxa.util import (
    FileWriter,
    Status,
    StatusOptions,
    chunked_transfer
)
from motor.motor_asyncio import AsyncIOMotorCollection

from commandcenter.dependencies.telalert import (
    get_telalert_client,
    get_dialout_collection,
    post_dialout
)
from commandcenter.telalert.client import TelAlertClient
from commandcenter.telalert.models import DialoutRequest
from commandcenter.telalert.stream import get_alerts
from commandcenter.settings import DIALOUT_SETTINGS



_LOGGER = logging.getLogger("commandcent.api.dialouts")

router = APIRouter(
    prefix="/dialout",
    tags=["DialOut"],
    dependencies=[
        Depends(
            requires(
                scopes=DIALOUT_SETTINGS.scopes,
                any_=DIALOUT_SETTINGS.any,
                raise_on_no_scopes=DIALOUT_SETTINGS.raise_on_no_scopes
            )
        )
    ]
)


@router.put("/alert", response_model=Status, dependencies=[Depends(can_write)])
async def alert(
    dialout: DialoutRequest,
    tasks: BackgroundTasks,
    post: bool = Depends(post_dialout),
    client: TelAlertClient = Depends(get_telalert_client)
) -> Status:
    """Send a dialout alert."""
    if not post:
        return Status(status=StatusOptions.FAILED)
    tasks.add_task(client.send_alert, dialout.message)
    return Status(status=StatusOptions.OK)


@router.get("/recorded", response_class=StreamingResponse, dependencies=[Depends(can_read)])
async def recorded(
    start_time: datetime = Depends(
        parse_timestamp(
            query=Query(default=None, alias="startTime"),
            default_timedelta=3600
        )
    ),
    end_time: datetime = Depends(
        parse_timestamp(
            query=Query(default=None, alias="endTime")
        )
    ),
    collection: AsyncIOMotorCollection = Depends(get_dialout_collection),
    file_writer: FileWriter = Depends(get_file_writer),
) -> StreamingResponse:
    """Get a batch of recorded alerts."""
    send = get_alerts(
        collection=collection,
        start_time=start_time,
        end_time=end_time
    )

    buffer, writer, suffix, media_type = (
        file_writer.buffer, file_writer.writer, file_writer.suffix, file_writer.media_type
    )

    chunk_size = 1000
    writer(
        [
            "timestamp", "posted_by", "service", "idempotency_key", "subject",
            "groups", "destinations", "msg"
        ]
    )
    filename = (
        f"{start_time.strftime('%Y%m%d%H%M%S')}-"
        f"{end_time.strftime('%Y%m%d%H%M%S')}-events{suffix}"
    )

    return StreamingResponse(
        chunked_transfer(
            send=send,
            buffer=buffer,
            writer=writer,
            formatter=None,
            logger=_LOGGER,
            chunk_size=chunk_size
        ),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
