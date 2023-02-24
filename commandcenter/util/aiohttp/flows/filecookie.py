import json
import os
import pathlib
from typing import AsyncGenerator

import toml
import yaml
from aiohttp import ClientRequest, ClientResponse
from aiohttp.connector import Connection

from commandcenter.util.aiohttp.auth import AuthFlow



class FileCookieAuthFlow(AuthFlow):
    """Auth flow for reading cookie authentication headers from a file.
    
    Args:
        path: The path to the file. File must be .json, .toml, or .yml.
    
    Raises:
        FileNotFoundError: File not found.
        ValueError: Invalid file type.
    """
    def __init__(self, path: os.PathLike) -> None:
        path = pathlib.Path(path)
        if not path.exists():
            raise FileNotFoundError(path.__str__())
        if path.suffix.lower() not in (".json", ".toml", ".yml"):
            raise ValueError(f"Invalid file type {path.suffix}.")
        self._path = path

    async def auth_flow(
        self,
        request: ClientRequest,
        _: Connection
    ) -> AsyncGenerator[None, ClientResponse]:
        match self._path.suffix.lower():
            case ".json":
                cookies = json.loads(self._path.read_text())
            case ".toml":
                cookies = toml.loads(self._path.read_text())
            case ".yml":
                cookies = yaml.safe_load(self._path.read_text())
        request.update_cookies(cookies)
        yield