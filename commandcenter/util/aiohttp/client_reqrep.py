from http.cookies import CookieError
from typing import Tuple, Type

from aiohttp import ClientRequest, ClientResponse, hdrs
from aiohttp.client_exceptions import ClientResponseError
from aiohttp.connector import Connection
from aiohttp.helpers import set_result
from aiohttp.http import HttpProcessingError
from aiohttp.log import client_logger

from commandcenter.util.aiohttp.auth import AuthError, AuthFlow



class ClientResponse_(ClientResponse):
    """Custom response handler for `aiohttp.ClientSession`"""
    async def start(self, connection: Connection, release: bool = True) -> None:
        self._closed = False
        self._protocol = connection.protocol
        self._connection = connection

        # when an auth flow completes, the response may have already been "started"
        # (i.e it will have a content attribute which is not None). ClientSession
        # will call `start` when the auth flow completes so we check for content
        # to avoid errors post auth flow
        if self.content is None:
            with self._timer:
                while True:
                    # read response
                    try:
                        protocol = self._protocol
                        message, payload = await protocol.read()  # type: ignore[union-attr]
                    except HttpProcessingError as exc:
                        raise ClientResponseError(
                            self.request_info,
                            self.history,
                            status=exc.code,
                            message=exc.message,
                            headers=exc.headers,
                        ) from exc
                    if message.code < 100 or message.code > 199 or message.code == 101:
                        break
                    if self._continue is not None:
                        set_result(self._continue, True)
                        self._continue = None

            # payload eof handler
            if release: # prevent releasing connection back to pool on rep complete
                payload.on_eof(self._response_eof)

            # response status
            self.version = message.version
            self.status = message.code
            self.reason = message.reason

            # headers
            self._headers = message.headers  # type is CIMultiDictProxy
            self._raw_headers = message.raw_headers  # type is Tuple[bytes, bytes]

            # payload
            self.content = payload

            # cookies
            for hdr in self.headers.getall(hdrs.SET_COOKIE, ()):
                try:
                    self.cookies.load(hdr)
                except CookieError as exc:
                    client_logger.warning("Can not load response cookies: %s", exc)
        return self

    def start_next_cycle(self) -> None:
        # this prevent the connection from being released when the __del__ method
        # is called (which could occurrs on each req/rep cycle)
        self._connection = None

    def release_on_read(self) -> None:
        # Reset the flags in the response so the connection is released on read
        if self.content is not None:
            self.content.on_eof(self._response_eof)
    

def auth_request(flow: AuthFlow) -> Type[ClientRequest]:
    class ClientRequest_(ClientRequest):
        """Custom request handler for `aiohttp.ClientSession`"""
        async def send(self, conn: Connection) -> ClientResponse_:
            auth_flow = flow.auth_flow(self, conn)
            try:
                await auth_flow.__anext__()
                while True:
                    resp = await self._send(conn)
                    try:
                        try:
                            await auth_flow.asend(resp)
                        except StopAsyncIteration:
                            resp.release_on_read()
                            return resp
                    except Exception as e:
                        await resp.close()
                        raise AuthError() from e
                    await resp.read()
                    resp.start_next_cycle()
            finally:
                await auth_flow.aclose()

        async def _send(self, conn: Connection) -> ClientResponse_:
            resp = await super().send(conn)
            try:
                return await resp.start(conn, release=False)
            except BaseException:
                resp.close()
                raise
    
    return ClientRequest_


def create_auth_handlers(flow: AuthFlow) -> Tuple[Type[ClientRequest], Type["ClientResponse_"]]:
    """Create a pair of `ClientRequest`/`ClientResponse` objects that can be
    used in an `aiohttp.ClientSession` with the authentication scheme.

    The req/rep classes should be passed to 'request_class' and 'response_class'
    arguments respectively.

    Args:
        flow: The auth flow to use for requests.

    Returns:
        handlers: request/response tuple of type `ClientRequest` and `ClientResponse`
            respectively.
    """
    return auth_request(flow), ClientResponse_