import concurrent.futures
import logging
import math
import socket
import subprocess
import sys
from typing import List, Tuple

from hyprxa.util import Timer

from commandcenter.auth.exceptions import NoHostsFound



_LOGGER = logging.getLogger("commandcenter.auth.discovery")


def discover_domain() -> str:
    """Get the domain name that the host machine is associated to.
    
    Raises:
        OSError: Originating from an error on the socket.
    """
    host = socket.gethostname()
    fqdn = socket.getaddrinfo(host, 0, flags=socket.AI_CANONNAME)[0][3]
    return fqdn[len(host)+1:].split(".")[0].upper()


def discover_domain_controllers(
    domain: str | None = None,
    port: int = 389,
    family: socket.AddressFamily = socket.AF_INET,
    sock_type: socket.SocketType = socket.SOCK_STREAM,
    protocol: int = 6,
    timeout: float = 0.25,
    mean_rtt_attempts: int = 3,
    max_workers: int = 4,
    limit: int = 3
) -> None:
    """Get all domain controller servers associated to a specific domain name.
    
    This function returns a list of the host names discovered sorted by mean RTT.
    By default, this function tests the LDAP port for all hostnames returned by
    the `nltest /dclist:{domain}` command. The default LDAP port is 389. You can
    specify a different port to test the connection.

    Args:
        domain: The domain name to search.
        port: The port to test the RTT to the host.
        family: The address family of the socket.
        sock_type: The socket type.
        protocol: The protocol number.
        timeout: The socket timeout before the host is determined to be unreachable.
        mean_rtt_attempts: The number of connections to make to a single host. The
            average time to connect between all attempts will be factored into
            the mean RTT.
        max_workers: The number of threads to use to test connections.
        limit: An optional limit of the number of hosts to return.

    Returns:
        hosts: The sorted list of hostnames.

    Raises:
        NoHostsFound: No hostnames were found for the given domain.
        OSError: Error calling nltest.
    """
    if sys.platform != "win32":
        raise RuntimeError("Domain controller discovery only available on windows platforms.")
        
    domain = domain or discover_domain()

    def list_domain_controllers() -> List[str]:
        """Retrieve a list of all domain servers associated to a domain name."""
        process = subprocess.Popen(
            ["nltest", f"/dclist:{domain}"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        try:
            potential_hosts = [
                line.decode().strip().split(" ")[0] for line in process.stdout.readlines()
            ]
            hosts = [host for host in potential_hosts if "." in host]
            if not hosts:
                errs = [line.decode().rstrip("\n\r") for line in process.stderr.readlines()]
                for err in errs:
                    _LOGGER.error(err)
            return hosts
        finally:
            try:
                process.terminate()
            except ProcessLookupError:
                pass
    
    hosts = list_domain_controllers()
    if not hosts:
        raise NoHostsFound(domain)

    _LOGGER.info("Discovered %i hosts for %s", len(hosts), domain)

    sock_params = (family, sock_type, protocol)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futs = [
            executor.submit(get_rtt, sock_params, (host, port), timeout, mean_rtt_attempts)
            for host in hosts
        ]
        concurrent.futures.wait(futs, return_when=concurrent.futures.ALL_COMPLETED)
        rtt = [fut.result() for fut in futs]
        
    _, sorted_ = (list(t) for t in zip(*sorted(zip(rtt, hosts))))
    _LOGGER.info("Selected primary host %s", sorted_[0])
    return sorted_[:limit]


def get_rtt(
    sock_params: Tuple[socket.AddressFamily, socket.SocketType, int],
    address: Tuple[str, int],
    timeout: float,
    mean_rtt_attempts: int
) -> float:
    """Get the mean RTT to the specified host.
    
    If we are unable to connect to a host, the RTT is `math.inf`.
    """
    rtt = []
    for _ in range(mean_rtt_attempts):
        sock = socket.socket(*sock_params)
        sock.settimeout(timeout)
        try:
            with Timer() as timer:
                try:
                    sock.connect(address)
                except socket.error:
                    _LOGGER.debug("Unable to connect to host %s", address[0])
                    return math.inf
            rtt.append(timer.elapsed)
        finally:
            sock.close()
    else:
        return sum(rtt)/len(rtt)