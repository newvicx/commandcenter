import inspect
from typing import List

from aiohttp import ClientSession, ClientTimeout, TCPConnector
from hyprxa.exceptions import NotConfiguredError
from hyprxa.util import format_docstring, DEFAULT_DATABASE
from pydantic import (
    AnyHttpUrl,
    BaseSettings,
    Field,
    FilePath,
    SecretStr,
    confloat,
    conint
)

from commandcenter.auth.backend import ActiveDirectoryBackend
from commandcenter.auth.client import ActiveDirectoryClient
from commandcenter.telalert.client import TelAlertClient
from commandcenter.sources.pi_web.integration import PIWebIntegration
from commandcenter.sources.pi_web.models import WebIdType
from commandcenter.sources.traxx.integration import TraxxIntegration
from commandcenter.types import AnyWsUrl
from commandcenter.util.aiohttp import (
    FileCookieAuthFlow,
    NegotiateAuth,
    create_auth_handlers
)



_AD = inspect.signature(ActiveDirectoryClient).parameters
_PWI = inspect.signature(PIWebIntegration).parameters
_TI = inspect.signature(TraxxIntegration).parameters
_TAL = inspect.signature(TelAlertClient).parameters


class AuthSettings(BaseSettings):
    domain: str = Field(
        default=_AD["domain"].default,
        description=format_docstring("""The name of the domain to connect to. If
        `None` the client will attempt to discover the domain the host is
        joined to. Defaults to '{}'""".format(_AD["domain"].default))
    )
    hosts: List[str] = Field(
        default=_AD["hosts"].default,
        description=format_docstring("""The domain controller hosts available to
        the client. If `None` the client will attempt to discover the available
        hosts. Auto host discovery is only available on Windows platforms. Defaults to '{}'
        """.format(_AD["hosts"].default))
    )
    tls: bool = Field(
        default=_AD["tls"].default,
        description=format_docstring("""If `True`, LDAP connection should use TLS.
        Defaults to `{}`""".format(_AD["tls"].default))
    )
    maxconn: conint(gt=0) = Field(
        default=_AD["maxconn"].default,
        description=format_docstring("""The maximum LDAP connection pool size.
        Defaults to `{}`""".format(_AD["maxconn"].default))
    )
    mechanism: str = Field(
        default=_AD["mechanism"].default,
        description=format_docstring("""The SASL mechanism used to authenticate
        with the domain controller server. Defaults to '{}'""".format(_AD["mechanism"].default))
    )
    username: str = Field(
        default=_AD["username"].default,
        description=format_docstring("""The username used to connect to the LDAP
        server during user queries. Defualts to '{}'""".format(_AD["username"].default))
    )
    password: SecretStr = Field(
        default=None,
        description=format_docstring("""The pasword used to connect to the LDAP
        server during user queries. Defaults to `None`""")
    )

    def get_client(self) -> ActiveDirectoryBackend:
        """Configure an ActiveDirectory and TokenHandler instance from settings."""
        return ActiveDirectoryClient(
            domain=self.domain,
            hosts=self.hosts,
            tls=self.tls,
            maxconn=self.maxconn,
            mechanism=self.mechanism,
            username=self.username,
            password=self.password
        )

    class Config:
        env_file=".env"
        env_prefix="auth_"


class PIWebIntegrationSettings(BaseSettings):
    base_url: AnyWsUrl = Field(
        default=None,
        description=format_docstring("""The base url to connect to the PI Web API.
        This should be a websocket url. If not set, an attempt to use the integration
        will raise `NotConfiguredError`.""")
    )
    auth_delegate: bool = Field(
        default=False,
        description=format_docstring("""Indicates that the user's credentials are
        to be delegated to the server. Defaults to `False`""")
    )
    auth_domain: str = Field(
        default=None,
        description=format_docstring("""NT Domain name. Defaults to `None` for
        local account.""")
    )
    auth_service: str = Field(
        default="HTTP",
        description=format_docstring("""Kerberos Service type for remote Service
        Principal Name. Defaults to 'HTTP'""")
    )
    auth_username: str = Field(
        default=None,
        description=format_docstring("""The username for authenticated requests.
        Defaults to `None` for local account.""")
    )
    auth_password: SecretStr = Field(
        default=None,
        description=format_docstring("""The password for authenticated requests.
        Defaults to `None`""")
    )
    auth_opportunistic_auth: bool = Field(
        default=False,
        description=format_docstring("""If `True` send the Kerberos token with
        the first request. This should only be used in trusted environments.
        Defaults to `False`""")
    )
    keepalive_timeout: confloat(gt=0) = Field(
        default=20,
        description=format_docstring("""The keepalive timeout (in seconds) for
        unused connections in the connection pool. Defaults to `20`""")
    )
    request_timeout: confloat(gt=0) = Field(
        default=10,
        description=format_docstring("""The time (in seconds) for a request to
        complete. Defaults to `10`""")
    )
    close_timeout: confloat(gt=0) = Field(
        default=_PWI["close_timeout"].default,
        description=format_docstring("""The time (in seconds) to wait for
        the websocket closing handshake. Defaults to `{}`""".format(_PWI["close_timeout"].default))
    )
    heartbeat: confloat(gt=0) = Field(
        default=_PWI["heartbeat"].default,
        description=format_docstring("""The heartbeat interval for the websocket
        connection. Defaults to `{}`""".format(_PWI["heartbeat"].default))
    )
    initial_backoff: confloat(gt=0) = Field(
        default=_PWI["initial_backoff"].default,
        description=format_docstring("""The minimum amount of time (in seconds)
        to wait before trying to reconnect to PI. Defaults to `{}`
        """.format(_PWI["initial_backoff"].default))
    )
    max_backoff: confloat(gt=0) = Field(
        default=_PWI["max_backoff"].default,
        description=format_docstring("""The maximum backoff time (in seconds) to
        wait before trying to reconnect to PI. Defaults to `{}`
        """.format(_PWI["max_backoff"].default))
    )
    max_reconnect_attempts: conint(gt=0) = Field(
        default=_PWI["max_reconnect_attempts"].default,
        description=format_docstring("""The maximum number of reconnect attempts
        before failing a `PIWebConnection` and dropping the subscriptions.
        Defaults to `{}`""".format(_PWI["max_reconnect_attempts"].default))
    )
    max_buffered_messages: conint(gt=0) = Field(
        default=_PWI["max_buffered_messages"].default,
        description=format_docstring("""The maximum number of messages that can
        be buffered on a `PIWebIntegration` instance before connections will
        not be able to publish to the integration. Defaults to `{}`
        """.format(_PWI["max_buffered_messages"].default))
    )
    max_connections: conint(gt=0) = Field(
        default=_PWI["max_connections"].default,
        description=format_docstring("""The maximum number of concurrent connections
        allowed to PI for a single integration instance. Defaults to `{}`
        """.format(_PWI["max_connections"].default))
    )
    max_subscriptions: conint(gt=0, le=50) = Field(
        default=_PWI["max_subscriptions"].default,
        description=format_docstring("""The maximum number of subscriptions that
        can be supported by a single `PIWebConnection`. This value multiplied
        by `max_connections` produces the total number of subscriptions a single
        `PIWebIntegration` instance can support. Defaults to `{}`""".format(_PWI["max_subscriptions"].default))
    )
    protocols: List[str] = Field(
        default=None,
        description=format_docstring("""Websocket protocols to use. Defaults to
        `None` (use default protocols).""")
    )
    web_id_type: WebIdType = Field(
        default=_PWI["web_id_type"].default,
        description=format_docstring("""The WebId type for subscriptions. The
        web id for the subscription must be of the specified type otherwise the
        integration will not work. Defaults to '{}'""".format(_PWI["web_id_type"].default))
    )
    max_msg_size: conint(ge=0) = Field(
        default=_PWI["max_msg_size"].default,
        description=format_docstring("""Maximum size of read websocket message.
        0 for no limit. Defaults to `{}`""".format(_PWI["max_msg_size"].default))
    )
    scopes: List[str] = Field(
        default_factory=list,
        description=format_docstring("""The required user scopes to access PI data.""")
    )
    any: bool = Field(
        default=True,
        description=format_docstring("""If `True`, a user with any of the PI Web
        scopes is authorized to read PI data. If `False` user needs all scopes.
        Defaults to `True`""")
    )
    raise_on_no_scopes: bool = Field(
        default=True,
        description=format_docstring("""If `True`, and no PI Web scopes are
        provided, a `NotConfiguredError` will be raised when trying to access a
        resource that requires PI Web scopes. Defaults to `True`""")
    )

    def get_integration(self) -> PIWebIntegration:
        """Configure a PIWebIntegration instance from settings."""
        if self.base_url is None:
            raise NotConfiguredError("PI Web integration settings not configured.")
        password = None
        if self.auth_password is not None:
            password = self.auth_password.get_secret_value()
        flow = NegotiateAuth(
            username=self.auth_username,
            password=password,
            domain=self.auth_domain,
            service=self.auth_service,
            delegate=self.auth_delegate,
            opportunistic_auth=self.auth_opportunistic_auth
        )
        request_class, response_class = create_auth_handlers(flow)
        session = ClientSession(
            base_url=self.base_url,
            connector=TCPConnector(
                limit=None,
                keepalive_timeout=self.keepalive_timeout
            ),
            request_class=request_class,
            response_class=response_class,
            timeout=ClientTimeout(total=self.request_timeout)
        )
        return PIWebIntegration(
            session=session,
            web_id_type=self.web_id_type,
            max_connections=self.max_connections,
            max_subscriptions=self.max_subscriptions,
            max_buffered_messages=self.max_buffered_messages,
            max_reconnect_attempts=self.max_reconnect_attempts,
            initial_backoff=self.initial_backoff,
            max_backoff=self.max_backoff,
            protocols=self.protocols,
            heartbeat=self.heartbeat,
            close_timeout=self.close_timeout,
            max_msg_size=self.max_msg_size
        )
    
    class Config:
        env_file=".env"
        env_prefix="sources_piweb_"


class TraxxIntegrationSettings(BaseSettings):
    base_url: AnyHttpUrl = Field(
        default=None,
        description=format_docstring("""The base url to connect to Traxx.
        This should be an HTTP url. If not set, an attempt to use the integration
        will raise `NotConfiguredError`.""")
    )
    cookie_path: FilePath = Field(
        default=None,
        description=format_docstring("""The path to a file which contains the
        session information to authenticate with Traxx. If not set, an attempt to
        use the integration will raise a `NotConfiguredError`""")
    )
    max_subscriptions: conint(gt=0) = Field(
        default=_TI["max_subscriptions"].default,
        description=format_docstring("""The maximum number of subscriptions that
        can be supported by a single `TraxxIntegration`. Defaults to `{}`
        """.format(_TI["max_subscriptions"].default))
    )
    max_buffered_messages: conint(gt=0) = Field(
        default=_TI["max_buffered_messages"].default,
        description=format_docstring("""The maximum number of messages that can
        be buffered on a `TraxxIntegration` instance before connections will
        not be able to publish to the integration. Defaults to `{}`
        """.format(_TI["max_buffered_messages"].default))
    )
    update_interval: confloat(ge=5) = Field(
        default=_TI["update_interval"].default,
        description=format_docstring("""The time (in seconds) between data updates
        for the traxx connection. This is also the max backoff for failed attempts.
        Defaults to `{}`""".format(_TI["update_interval"].default))
    )
    max_missed_updates: conint(gt=0) = Field(
        default=_TI["max_missed_updates"].default,
        description=format_docstring("""The maximum number of missed updates in
        a row before failing the `TraxxConnection` and dropping the subscriptions.
        Defaults to `{}`""".format(_TI["max_missed_updates"].default))
    )
    initial_backoff: confloat(gt=0) = Field(
        default=_TI["initial_backoff"].default,
        description=format_docstring("""The minimum amount of time (in seconds)
        to wait before trying to reconnect to Traxx. Defaults to `{}`
        """.format(_TI["initial_backoff"].default))
    )
    keepalive_timeout: confloat(gt=0) = Field(
        default=20,
        description=format_docstring("""The keepalive timeout (in seconds) for
        unused connections in the connection pool. Defaults to `20`""")
    )
    request_timeout: confloat(gt=0) = Field(
        default=10,
        description=format_docstring("""The time (in seconds) for a request to
        complete. Defaults to `10`""")
    )
    max_connections: conint(gt=0) = Field(
        default=10,
        description=format_docstring("""The maximum number of concurrent connections
        to Traxx. This does not impact the total number of subscriptions allowed.
        Defaults to `10`""")
    )
    scopes: List[str] = Field(
        default_factory=list,
        description=format_docstring("""The required user scopes to access Traxx data.""")
    )
    any: bool = Field(
        default=True,
        description=format_docstring("""If `True`, a user with any of the Traxx
        scopes is authorized to read Traxx data. If `False` user needs all scopes.
        Defaults to `True`""")
    )
    raise_on_no_scopes: bool = Field(
        default=False,
        description=format_docstring("""If `True`, and no Traxx scopes are
        provided, a `NotConfiguredError` will be raised when trying to access a
        resource that requires Traxx scopes. Defaults to `False`""")
    )

    def get_integration(self) -> TraxxIntegration:
        """Configure a TraxxIntegration instance from settings."""
        if self.base_url is None or self.cookie_path is None:
            raise NotConfiguredError("Traxx integration settings not configured.")
        flow = FileCookieAuthFlow(path=self.cookie_path)
        request_class, response_class = create_auth_handlers(flow)
        session = ClientSession(
            base_url=self.base_url,
            connector=TCPConnector(
                limit=self.max_connections,
                keepalive_timeout=self.keepalive_timeout
            ),
            request_class=request_class,
            response_class=response_class,
            timeout=ClientTimeout(total=self.request_timeout)
        )
        return TraxxIntegration(
            session=session,
            max_subscriptions=self.max_subscriptions,
            max_buffered_messages=self.max_buffered_messages,
            update_interval=self.update_interval,
            max_missed_updates=self.max_missed_updates,
            initial_backoff=self.initial_backoff
        )

    class Config:
        env_file=".env"
        env_prefix="sources_traxx_"


class TelAlertSettings(BaseSettings):
    database_name: str = Field(
        default=DEFAULT_DATABASE,
        description=format_docstring("""The MongoDB database to connect to.
        Defaults to '{}'""".format(DEFAULT_DATABASE))
    )
    collection_name: str | None = Field(
        default="dialouts",
        description=format_docstring("""The MongoDB collection to store dialout
        data. Defaults to 'dialouts'""")
    )
    path: FilePath = Field(
        default=None,
        description=format_docstring("""The path to the 'telalert.exe' application.
        If not set, an attempt to use the dial out API will raise a `NotConfiguredError`""")
    )
    host: str = Field(
        default=None,
        description=format_docstring("""The target host for dialout requests.
        If not set, an attempt to use the dial out API will raise a `NotConfiguredError`""")
    )
    max_workers: conint(gt=0, le=10) = Field(
        default=_TAL["max_workers"].default,
        description=format_docstring("""The maximum number of subprocesses that
        can run concurrently. This limits the total number of working processes
        sending requests to the TelAlert server. Defaults to `{}`""".format(_TAL["max_workers"].default))
    )
    timoeut: confloat(gt=0) = Field(
        default=_TAL["timeout"].default,
        description=format_docstring("""The maximum time (in seconds) for a call
        to 'telalert.exe' to complete. Defaults to `{}`""".format(_TAL["timeout"].default))
    )
    scopes: List[str] = Field(
        default_factory=list,
        description=format_docstring("""The required user scopes to send dialouts.""")
    )
    any: bool = Field(
        default=True,
        description=format_docstring("""If `True`, a user with any of the dialout
        scopes is authorized to send dialouts. If `False` user needs all scopes.
        Defaults to `True`""")
    )
    raise_on_no_scopes: bool = Field(
        default=True,
        description=format_docstring("""If `True`, and no dialout scopes are
        provided, a `NotConfiguredError` will be raised when trying to access a
        resource that requires dialout scopes. Defaults to `True`""")
    )

    def get_client(self) -> TelAlertClient:
        """Configure a TelAlertClient instance from settings."""
        if self.path is None or self.host is None:
            raise NotConfiguredError("Dialout settings not configured.")
        return TelAlertClient(
            path=self.path,
            host=self.host,
            max_workers=self.max_workers,
            timeout=self.timoeut
        )

    class Config:
        env_file=".env"
        env_prefix="dialout_"


AUTH_SETTINGS = AuthSettings()
PIWEB_SETTINGS = PIWebIntegrationSettings()
TRAXX_SETTINGS = TraxxIntegrationSettings()
DIALOUT_SETTINGS = TelAlertSettings()