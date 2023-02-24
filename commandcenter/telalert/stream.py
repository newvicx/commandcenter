from collections.abc import AsyncIterable
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorCollection

from commandcenter.telalert.models import DialoutDocument, ValidatedDialoutDocument



async def get_alerts(
    collection: AsyncIOMotorCollection,
    start_time: datetime,
    end_time: datetime | None = None,
) -> AsyncIterable[DialoutDocument]:
    """Stream alerts in a time range.
    
    Args:
        collection: The motor collection.
        start_time: Start time of query. This is inclusive.
        end_time: End time of query. This is inclusive.
    
    Yields:
        row: A dialout row
    
    Raises:
        ValueError: If 'start_time' >= 'end_time'.
        PyMongoError: Error in motor client.
    """
    end_time = end_time or datetime.utcnow().replace(tzinfo=None)
    if start_time >= end_time:
        raise ValueError("'start_time' cannot be greater than or equal to 'end_time'")
    query = {"timestamp": {"$gte": start_time, "$lte": end_time}}
    async for alert in collection.find(
        query,
        projection={"_id": 0}
    ).sort("timestamp", 1):
        validated: DialoutDocument = ValidatedDialoutDocument(**alert)
        row = (
            validated.timestamp.isoformat(),
            validated.posted_by,
            validated.service,
            validated.idempotency_key,
            validated.message.get("subject"),
            validated.message.get("groups"),
            validated.message.get("destinations"),
            validated.message.get("msg")
        )
        yield row