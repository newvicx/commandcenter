import base64
import hashlib
import json
import socket
import struct
from typing import AsyncGenerator, Tuple, Union

import pywintypes
import sspi
import sspicon
import win32security
from aiohttp import ClientRequest, ClientResponse, hdrs
from aiohttp.connector import Connection
from aiohttp.log import client_logger

from commandcenter.util.aiohttp.auth import AuthFlow



class NegotiateAuth(AuthFlow):
    """Negotiate (Kerberos/NTLM) flow.
    
    Args:
        username: Username.
        password: Password.
        domain: NT Domain name. Default: '.' for local account.
        service: Kerberos Service type for remote Service Principal Name. Default: 'HTTP'
        host: Host name for Service Principal Name. Default: Extracted from request URI
        delegate: Indicates that the user's credentials are to be delegated to the server.
            Default: False
        opportunistic_auth: If `True` send the Kerberos token with the first
            request. This should only be used in trusted environments.
    
    If username and password are not specified, the user's default credentials
    are used. This allows for single-sign-on to domain resources if the user is
    currently logged on with a domain account.
    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
        domain: str = None,
        service: str = 'HTTP',
        delegate: bool = False,
        opportunistic_auth: bool = False
    ):
        auth_info: Union[Tuple[str, str, str], None] = None
        if username is not None:
            auth_info = (username, domain, password)
        self._auth_info = auth_info
        self._service = service
        self._delegate = delegate
        self._opportunistic_auth = opportunistic_auth

    async def auth_flow(
        self,
        request: ClientRequest,
        connection: Connection
    ) -> AsyncGenerator[None, ClientResponse]:
        """Implementation of the Negotiate/NTLM challenge-response authentication
        protocol.
        """
        request.headers.pop(hdrs.AUTHORIZATION, None)
        if not self._opportunistic_auth:
            response = yield
            if response.status != 401:
                return
            allowed_schemes = ("Negotiate", "NTLM")
            proposed_schemes = response.headers.get(hdrs.WWW_AUTHENTICATE)
            if proposed_schemes is None:
                return
            scheme = None
            for proposed_scheme in proposed_schemes.split(','):
                if proposed_scheme.strip() in allowed_schemes:
                    scheme = proposed_scheme.strip()
                    break
            if scheme is None:
                return
        else:
            scheme = 'Negotiate'
        host = request.headers.pop(hdrs.HOST)
        try:
            host = socket.getaddrinfo(
                host, None, 0, 0, 0, socket.AI_CANONNAME
            )[0][3]
        except socket.gaierror as err:
            client_logger.info(
                'Skipping canonicalization of name %s due to error: %r',
                host,
                err
            )
        finally:
            request.headers[hdrs.HOST] = host
        
        # initialize security buffer and sspi ClientAuth
        targetspn = '{}/{}'.format(self._service, host)
        scflags = sspicon.ISC_REQ_MUTUAL_AUTH
        if self._delegate:
            scflags |= sspicon.ISC_REQ_DELEGATE
        pkg_info = win32security.QuerySecurityPackageInfo(scheme)
        clientauth = sspi.ClientAuth( 
            scheme,
            targetspn=targetspn,
            auth_info=self._auth_info,
            scflags=scflags,
            datarep=sspicon.SECURITY_NETWORK_DREP
        )
        sec_buffer = win32security.PySecBufferDescType()
        
        peercert = connection.transport.get_extra_info('peercert')
        # prep auth header for Kerberos auth
        if peercert is not None:
            # do channel binding hash, required for SSL connections
            peercert_b = json.dumps(peercert).encode()
            md = hashlib.sha256()
            md.update(peercert_b)
            appdata = 'tls-server-end-point:'.encode('ASCII') + md.digest()
            cbtbuf = win32security.PySecBufferType(
                pkg_info['MaxToken'], sspicon.SECBUFFER_CHANNEL_BINDINGS
            )
            cbtbuf.Buffer = struct.pack(
                'LLLLLLLL{}s'.format(len(appdata)),
                0, 0, 0, 0, 0, 0,
                len(appdata),
                32,
                appdata
            )
            sec_buffer.append(cbtbuf)
        
        try:
            # try Kerberos authentication
            error, auth = clientauth.authorize(sec_buffer) 
            auth_header = f"{scheme} {base64.b64encode(auth[0].Buffer).decode('ASCII')}" 
            client_logger.debug(
                'Sending Initial Context Token - error=%s authenticated=%s',
                error,
                clientauth.authenticated 
            )
        except pywintypes.error as err:
            client_logger.debug("Error in client auth: %r", repr(err))
            raise
        else:
            request.headers[hdrs.AUTHORIZATION] = auth_header
        
        response = yield
        
        if response.status != 401: # Kerberos succeeded
            # finalize our auth context
            final_context = response.headers.get(hdrs.WWW_AUTHENTICATE)
            if final_context is not None:
                try:
                    # Sometimes Windows seems to forget to prepend 'Negotiate'
                    # to the success response, and we get just a bare chunk of
                    # base64 token. Not sure why.
                    final_context = final_context.replace(scheme, '', 1).lstrip()
                    tokenbuf = win32security.PySecBufferType(
                        pkg_info['MaxToken'],
                        sspicon.SECBUFFER_TOKEN
                    )
                    tokenbuf.Buffer = base64.b64decode(final_context.encode('ASCII'))
                    sec_buffer.append(tokenbuf)
                    error, _ = clientauth.authorize(sec_buffer) 
                    client_logger.debug(
                        "Kerberos Authentication succeeded - error=%s authenticated=%s",
                        error, clientauth.authenticated 
                    )
                except TypeError:
                    pass
            return
        
        # try NTLM
        challenge_header = response.headers.get(hdrs.WWW_AUTHENTICATE)
        challenge = [
            val[len(scheme)+1:] for val in challenge_header.split(', ')
            if scheme in val
        ]
        if len(challenge) > 1:
            response.close()
            raise ValueError(
                f"Did not get exactly one {scheme} challenge from server."
            )
        tokenbuf = win32security.PySecBufferType(
            pkg_info['MaxToken'],
            sspicon.SECBUFFER_TOKEN
        )
        tokenbuf.Buffer = base64.b64decode(challenge[0])
        sec_buffer.append(tokenbuf)
        
        try:
            error, auth = clientauth.authorize(sec_buffer) 
            auth_header = f"{scheme} {base64.b64encode(auth[0].Buffer).decode('ASCII')}" 
            client_logger.debug(
                'Sending challenge response - error=%s authenticated=%s',
                error,
                clientauth.authenticated 
            )
        except pywintypes.error as err:
            client_logger.debug("Error in client auth: %r", repr(err))
            raise
        else:
            request.headers[hdrs.AUTHORIZATION] = auth_header
        
        yield
        return