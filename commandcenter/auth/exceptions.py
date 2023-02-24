from hyprxa.exceptions import AuthError



class ActiveDirectoryError(AuthError):
    """Base exception for AD related errors."""


class NoHostsFound(ActiveDirectoryError):
    """Raised when no domain server hosts are found for a given domain name."""
    def __init__(self, domain: str) -> None:
        self.domain = domain

    def __str__(self) -> str:
        return "No hosts found for {}.".format(self.domain)