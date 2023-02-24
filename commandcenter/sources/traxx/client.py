from typing import Dict, Type

from aiohttp import ClientSession
from hyprxa.util import DEFAULT_TIMEZONE
from uplink import AiohttpClient, Consumer, Query, get, headers



@headers({"Accept": "text/csv"})
class Sensors(Consumer):
    @get("/assets/{asset_id}/asset_sensor_profiles/{sensor_id}/measurements/query.csv")
    def sensor_data(
        self,
        asset_id: int,
        sensor_id: str,
        begin: Query,
        end: Query,
        tz: Query = DEFAULT_TIMEZONE
    ):
        """Get sensor data for an asset as a CSV."""


class TraxxHttpClient:
    """HTTP Traxx client."""
    consumers: Dict[str, Consumer] = {}

    def __init__(self, session: ClientSession) -> None:
        self.session = session

    @property
    def sensors(self) -> Sensors:
        return self._get_consumer_instance(Sensors)

    async def close(self) -> None:
        await self.session.close()
    
    def _get_consumer_instance(self, consumer: Type[Consumer]) -> Consumer:
        """Get an instance of the consumer for the a controller.
        
        This caches the consumer instance in the class for reuse.
        """
        name = consumer.__name__
        if name in self.consumers:
            return self.consumers[name]
        instance = consumer(client=AiohttpClient(session=self.session))
        self.consumers[name] = instance
        return instance