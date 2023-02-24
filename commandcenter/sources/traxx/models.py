from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Union

import orjson
from hyprxa.timeseries import (
    BaseSourceSubscription,
    BaseSourceSubscriptionRequest,
    SubscriptionMessage
)
from hyprxa.util import isoparse
from hyprxa.util.models import BaseModel
from pendulum.datetime import DateTime
from pydantic import BaseModel, validator



class SensorTypes(str, Enum):
    """Available sensor types in Traxx."""
    PROXIMITY = "proximity"
    VOLTS = "volts"
    TEMP = "temp"
    CURRENT = "current"
    AMBIENT = "ambient"


class TraxxSubscription(BaseSourceSubscription):
    """Model for all Traxx subscriptions."""
    asset_id: int
    sensor_id: str
    sensor_type: SensorTypes = SensorTypes.PROXIMITY
    source: str = "traxx"

    @property
    def key(self) -> str:
        return f"{self.asset_id}-{self.sensor_id}"


class TraxxSubscriptionRequest(BaseSourceSubscriptionRequest):
    """Model for PI Web subscription requests."""
    subscriptions: List[TraxxSubscription]


class BaseTraxxSensorMessage(BaseModel):
    """Base message for data returned from Traxx."""
    class Config:
        extra="ignore"
        allow_arbitrary_types=True


class TraxxSensorItem(BaseTraxxSensorMessage):
    """Model for a single sample from Traxx."""
    timestamp: DateTime
    value: float

    @validator('timestamp', pre=True)
    def _parse_timestamp(cls, v: str) -> DateTime:
        """Parse timestamp (str) to DateTime."""
        if not isinstance(v, str):
            raise TypeError("Expected type str")
        try:
            return isoparse(v.split("+")[0].strip()).replace(tzinfo=None)
        except Exception as err:
            raise ValueError("Cannot parse timestamp") from err

    def json(self, *args, **kwargs) -> str:
        return orjson.dumps(self.dict(*args, **kwargs)).decode()

    def dict(self, *args, **kwargs) -> Dict[str, Union[str, Any]]:
        return {"timestamp": self.timestamp.isoformat(), "value": self.value}

    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TraxxSensorItem)):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, TraxxSensorItem):
            return self.timestamp > __o.timestamp 
        else:
            return self.timestamp > __o

    def __ge__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TraxxSensorItem)):
            raise TypeError(f"'>=' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, TraxxSensorItem):
            return self.timestamp >= __o.timestamp 
        else:
            return self.timestamp >= __o

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TraxxSensorItem)):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, TraxxSensorItem):
            return self.timestamp < __o.timestamp 
        else:
            return self.timestamp < __o

    def __le__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, TraxxSensorItem)):
            raise TypeError(f"'<=' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, TraxxSensorItem):
            return self.timestamp <= __o.timestamp 
        else:
            return self.timestamp <= __o


class TraxxSensorMessage(BaseTraxxSensorMessage):
    """Model for Traxx client message."""
    subscription: TraxxSubscription
    items: List[TraxxSensorItem]

    def filter(self, last_timestamp: datetime) -> None:
        self.items = [item for item in self.items if item.timestamp > last_timestamp]


class TraxxSubscriptionMessage(SubscriptionMessage):
    """Model for Traxx subscription message."""
    subscription: TraxxSubscription