from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Union

import orjson
from hyprxa.timeseries import (
    BaseSourceSubscription,
    BaseSourceSubscriptionRequest,
    SubscriptionMessage
)
from hyprxa.util import in_timezone, isoparse, snake_to_camel
from hyprxa.util.models import BaseModel
from pendulum.datetime import DateTime
from pydantic import BaseModel, root_validator, validator



# TODO: Expand to AF elements and attributes
def restrict_obj_type(v: str) -> str:
    if v != PIWebObjType.POINT:
        raise ValueError("Only obj_type 'point' is currently supported")
    return v


class WebIdType(str, Enum):
    """https://docs.aveva.com/bundle/pi-web-api-reference/page/help/topics/webid-type.html"""
    FULL = "Full"
    IDONLY = "IDOnly"
    PATHONLY = "PathOnly"
    LOCALIDONLY = "LocalIDOnly"
    DEFAULTIDONLY = "DefaultIDOnly"


class PIWebObjType(str, Enum):
    POINT = "point"
    ATTRIBUTE = "attribute"
    ELEMENT = "element"


class PIWebSubscription(BaseSourceSubscription):
    """Model for all PI object subscriptions."""
    web_id: str
    name: str | None = None
    web_id_type: WebIdType = WebIdType.FULL
    obj_type: PIWebObjType = PIWebObjType.POINT
    source: str = "pi_web"
    
    validator("obj_type", allow_reuse=True)(restrict_obj_type)

    @validator("source")
    def _restrict_source(cls, v: str) -> str:
        v = v.lower().strip()
        if v != "pi_web":
            raise ValueError(f"Invalid source '{v}' for {cls.__class__.__name__}")
        return v


class PIWebSubscriptionRequest(BaseSourceSubscriptionRequest):
    """Model for PI Web subscription requests."""
    subscriptions: List[PIWebSubscription]


class PIWebBaseChannelMessage(BaseModel):
    """
    Base message for data returned from 'streamsets/channel' endpoint.

    ```
    Sample Message (Raw)
    {
    "Links": {},
    "Items": [
        {
        "WebId": "F1DPEmoryo_bV0GzilxLXH31pgjowAAAQUJDX1BJX09QU1xBSUM2ODEwNTkuUFY",
        "Name": "AIC681059.PV",
        "Path": "\\\\ABC_PI_Ops\\AIC681059.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2022-12-09T14:06:36Z",
            "Value": 50.40764,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        },
        {
        "WebId": "F1DPEmoryo_bV0GzilxLXH31pgcAQAAAQUJDX1BJX09QU1xGSVExNDAxMi5QVg",
        "Name": "FIQ14012.PV",
        "Path": "\\\\ABC_PI_Ops\\FIQ14012.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2022-12-09T14:06:37Z",
            "Value": 65229868.0,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        },
        {
        "WebId": "F1DPEmoryo_bV0GzilxLXH31pgLAcAAAQUJDX1BJX09QU1xUSTE0MDEzLlBW",
        "Name": "TI14013.PV",
        "Path": "\\\\ABC_PI_Ops\\TI14013.PV",
        "Links": {},
        "Items": [
            {
            "Timestamp": "2022-12-09T14:06:38.4350128Z",
            "Value": 73.68963,
            "UnitsAbbreviation": "",
            "Good": true,
            "Questionable": false,
            "Substituted": false,
            "Annotated": false
            }
        ],
        "UnitsAbbreviation": ""
        }
    ]
    }
    ```
    Target conversion to
    ```
    [
        {
            'name': 'AIC681059.PV',
            'web_id': 'F1DPEmoryo_bV0GzilxLXH31pgjowAAAQUJDX1BJX09QU1xBSUM2ODEwNTkuUFY',
            'items': [
                {'timestamp': '2022-12-09T09:06:36', 'value': 50.40764}
            ]
        },
        {
            'name': 'FIQ14012.PV',
            'web_id': 'F1DPEmoryo_bV0GzilxLXH31pgcAQAAAQUJDX1BJX09QU1xGSVExNDAxMi5QVg',
            'items': [
                {'timestamp': '2022-12-09T09:06:37', 'value': 65229868.0}
            ]
        },
        {
            'name': 'TI14013.PV',
            'web_id': 'F1DPEmoryo_bV0GzilxLXH31pgLAcAAAQUJDX1BJX09QU1xUSTE0MDEzLlBW',
            'items': [
                {'timestamp': '2022-12-09T09:06:38.435012', 'value': 73.68963}
            ]
        }
    ]
    ```
    """
    class Config:
        alias_generator = snake_to_camel
        extra = 'ignore'
        allow_arbitrary_types=True


class PIWebMessageSubItem(PIWebBaseChannelMessage):
    """Model for sub items containing timeseries data for a particular WebId."""
    timestamp: DateTime
    value: Any
    good: bool

    @validator('timestamp', pre=True)
    def _parse_timestamp(cls, v: str) -> DateTime:
        """Parse timestamp (str) to DateTime."""
        if not isinstance(v, str):
            raise TypeError("Expected type str")
        try:
            return isoparse(v).replace(tzinfo=None)
        except Exception as err:
            raise ValueError("Cannot parse timestamp") from err

    @root_validator
    def _format_content(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Replace value of not 'good' items with `None`."""
        value, good = v.get("value"), v.get("good")
        if not good:
            v["value"] = None
        elif value is not None:
            if isinstance(value, dict):
                v["value"] = value["Name"]
        
        return v

    def json(self, *args, **kwargs) -> str:
        return orjson.dumps(self.dict(*args, **kwargs)).decode()

    def dict(self, *args, **kwargs) -> Dict[str, Union[str, Any]]:
        return {"timestamp": self.timestamp.isoformat(), "value": self.value}

    def __gt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, PIWebMessageSubItem)):
            raise TypeError(f"'>' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, PIWebMessageSubItem):
            return self.timestamp > __o.timestamp 
        else:
            return self.timestamp > __o

    def __ge__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, PIWebMessageSubItem)):
            raise TypeError(f"'>=' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, PIWebMessageSubItem):
            return self.timestamp >= __o.timestamp 
        else:
            return self.timestamp >= __o

    def __lt__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, PIWebMessageSubItem)):
            raise TypeError(f"'<' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, PIWebMessageSubItem):
            return self.timestamp < __o.timestamp 
        else:
            return self.timestamp < __o

    def __le__(self, __o: object) -> bool:
        if not isinstance(__o, (datetime, PIWebMessageSubItem)):
            raise TypeError(f"'<=' not supported between instances of {type(self)} and {type(__o)}")
        if isinstance(__o, PIWebMessageSubItem):
            return self.timestamp <= __o.timestamp 
        else:
            return self.timestamp <= __o


class PIWebMessageItem(PIWebBaseChannelMessage):
    """Model for single top level item pertaining to a WebId."""
    name: str
    web_id: str
    items: List[PIWebMessageSubItem]

    @validator("items")
    def _sort_items(cls, v: List[PIWebMessageSubItem]) -> List[PIWebMessageSubItem]:
        # Timeseries values for a given WebId are not guarenteed to be in
        # chronological order so we sort the items on the timestamp to ensure
        # they are
        # https://docs.osisoft.com/bundle/pi-web-api-reference/page/help/topics/channels.html
        return sorted(v)


class PIWebMessage(PIWebBaseChannelMessage):
    """Model for streamset messages received from PI Web API."""
    items: List[PIWebMessageItem]


class PIWebSubscriptionMessage(SubscriptionMessage):
    """Model for PI Web subscription messages."""
    subscription: PIWebSubscription