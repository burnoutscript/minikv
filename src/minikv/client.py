"""Cliente RESP síncrono mínimo, só com a stdlib."""

from __future__ import annotations

import socket
from typing import Any

from .resp import NEED_MORE, RespError, RespParser, encode_command

Arg = bytes | str | int | float


class Client:
    """Conexão TCP com um servidor minikv (ou Redis)."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6380, timeout: float = 5.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._parser = RespParser()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._sock.close()

    def _read_reply(self) -> Any:
        while True:
            reply = self._parser.get()
            if reply is not NEED_MORE:
                return reply
            data = self._sock.recv(65536)
            if not data:
                raise ConnectionError("servidor fechou a conexão")
            self._parser.feed(data)

    def execute(self, *args: Arg) -> Any:
        """Envia um comando e devolve a resposta; erros do servidor viram RespError."""
        self._sock.sendall(encode_command(*args))
        reply = self._read_reply()
        if isinstance(reply, RespError):
            raise reply
        return reply

    def pipeline(self, *commands: tuple[Arg, ...]) -> list[Any]:
        """Envia vários comandos de uma vez e lê as respostas na ordem; erros vêm como valor."""
        self._sock.sendall(b"".join(encode_command(*cmd) for cmd in commands))
        return [self._read_reply() for _ in commands]

    def ping(self) -> bool:
        return self.execute("PING") == "PONG"

    def get(self, key: Arg) -> bytes | None:
        return self.execute("GET", key)

    def set(self, key: Arg, value: Arg, ex: int | None = None) -> bool:
        args: tuple[Arg, ...] = ("SET", key, value)
        if ex is not None:
            args += ("EX", ex)
        return self.execute(*args) == "OK"

    def delete(self, *keys: Arg) -> int:
        return self.execute("DEL", *keys)
