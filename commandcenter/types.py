from pydantic import AnyUrl



class AnyWsUrl(AnyUrl):
    allowed_schemes = {"ws", "wss"}

    __slots__ = ()