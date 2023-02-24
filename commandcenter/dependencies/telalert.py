from dataclasses import asdict

import pymongo
from fastapi import Depends, HTTPException, status
from hyprxa.dependencies import get_mongo_client
from hyprxa.caching import singleton
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import DuplicateKeyError

from commandcenter.telalert.client import TelAlertClient
from commandcenter.telalert.models import DialoutRequest
from commandcenter.settings import DIALOUT_SETTINGS



async def get_dialout_collection(
    db: AsyncIOMotorClient = Depends(get_mongo_client)
) -> AsyncIOMotorCollection:
    """Returns the dialout collection to perform operations against."""
    collection = db[DIALOUT_SETTINGS.database_name][DIALOUT_SETTINGS.collection_name]
    await collection.create_index([("idempotency_key", pymongo.ASCENDING)], unique=True)
    return collection


async def post_dialout(
    dialout: DialoutRequest,
    collection: AsyncIOMotorCollection = Depends(get_dialout_collection)
) -> bool:
    """Try to insert a dialout into the database.
    
    If the operation fails, the idempotency key is a duplicate and we will not
    post the dialout, return `False`. Otherwise, return `True`."""
    document = dialout.to_document()
    try:
        await collection.insert_one(asdict(document))
    except DuplicateKeyError:
        return False
    return True


@singleton
async def get_telalert_client() -> TelAlertClient:
    try:
        return DIALOUT_SETTINGS.get_client()
    except FileNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Application cannot connect to dialout server."
        )