import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict

import pydantic
from hyprxa.util import get_user_identity
from pydantic import BaseModel, Field, root_validator




class TelAlertMessage(BaseModel):
    """Model for a TelAlert message."""
    msg: str
    groups: Sequence[str] | None = Field(default_factory=list)
    destinations: Sequence[str] | None = Field(default_factory=list)
    subject: str | None
    
    @root_validator
    def _validate_message(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure that at least 1 of 'groups' or 'destinations' is present."""
        groups = v["groups"]
        destinations = v["destinations"]
        if not groups and not destinations:
            raise ValueError("Must provide either a group or destination to deliver message")
        return v


@dataclass
class DialoutDocument:
    message: Dict[str, Any]
    idempotency_key: str
    service: str = "telalert"
    posted_by: str = field(default_factory=get_user_identity)
    timestamp: datetime = field(default_factory=datetime.utcnow)


ValidatedDialoutDocument = pydantic.dataclasses.dataclass(DialoutDocument)


class DialoutRequest(BaseModel):
    """Dialout request model."""
    message: TelAlertMessage
    idempotency_key: str = Field(default_factory=lambda: uuid.uuid4().hex)

    def to_document(self) -> DialoutDocument:
        """Convert dialout request to a DialoutDocument for storage."""
        return DialoutDocument(
            message=self.message.dict(),
            idempotency_key=self.idempotency_key
        )