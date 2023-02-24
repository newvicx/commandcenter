from typing import AsyncGenerator

from aiohttp import ClientError, ClientRequest, ClientResponse
from aiohttp.connector import Connection


class AuthError(ClientError):
    """Subclass of client error. Raised from an exception in an auth flow."""


class AuthFlow:
    """Base auth flow implementation for an `aiohttp.ClientSession`. This brings
    HTTPX style auth flows to aiohttp.
    
    Auth flows are used as part of custom req/rep handler classes for the
    session. Flows are async generator objects that yield requests and receive
    back response objects. Implementations can manipulate headers, cookies,
    etc before sending the next request.
    
    This framework allows for authentication schemes not natively supported in
    aiohttp to be used.
    """
    async def auth_flow(
        self,
        request: ClientRequest,
        connection: Connection
    ) -> AsyncGenerator[None, ClientResponse]:
        # Equivalent to a no auth, send request as is
        yield