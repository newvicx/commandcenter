from hyprxa.timeseries import TimeseriesError



class TraxxExpiredSession(TimeseriesError):
    """Raised when the session cookie is expired and we can no longer authenticate
    with the server.
    """

    def __str__(self) -> str:
        return "Signed out. Please refresh session config"