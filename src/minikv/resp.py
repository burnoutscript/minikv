"""Parser incremental e codificação do RESP2, o protocolo do Redis."""

from __future__ import annotations

import re
from typing import Any

CRLF = b"\r\n"
MAX_BULK_SIZE = 512 * 1024 * 1024
MAX_INLINE_SIZE = 64 * 1024
MAX_DEPTH = 32
_INTEGER = re.compile(rb"-?[0-9]+")


class ProtocolError(Exception):
    """Bytes recebidos não formam RESP válido."""


class RespError(Exception):
    """Resposta de erro (-ERR ...) enviada pelo servidor."""


class _Incomplete(Exception):
    pass


class _NeedMore:
    def __repr__(self) -> str:
        return "NEED_MORE"


NEED_MORE: Any = _NeedMore()


class RespParser:
    """Acumula bytes da conexão e devolve um valor RESP completo por vez.

    Tipos devolvidos: simple string -> str, erro -> RespError, inteiro -> int,
    bulk string -> bytes, array -> list, nulo -> None. Comandos inline (texto puro,
    como num telnet) viram uma lista de bytes.
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> None:
        self._buf += data

    def get(self) -> Any:
        """Próximo valor completo, ou NEED_MORE se o buffer ainda não tem um."""
        try:
            value, pos = self._parse(0, 0)
        except _Incomplete:
            return NEED_MORE
        del self._buf[:pos]
        return value

    def __iter__(self):
        while (value := self.get()) is not NEED_MORE:
            yield value

    def _line(self, pos: int) -> tuple[bytes, int]:
        end = self._buf.find(CRLF, pos)
        if end < 0:
            if len(self._buf) - pos > MAX_INLINE_SIZE:
                raise ProtocolError("linha longa demais")
            raise _Incomplete
        return bytes(self._buf[pos:end]), end + 2

    def _int(self, pos: int) -> tuple[int, int]:
        line, pos = self._line(pos)
        if not _INTEGER.fullmatch(line):
            raise ProtocolError(f"inteiro inválido: {line!r}")
        return int(line), pos

    def _parse(self, pos: int, depth: int) -> tuple[Any, int]:
        if pos >= len(self._buf):
            raise _Incomplete
        prefix = self._buf[pos : pos + 1]
        if prefix == b"+":
            line, pos = self._line(pos + 1)
            return line.decode("utf-8", "replace"), pos
        if prefix == b"-":
            line, pos = self._line(pos + 1)
            return RespError(line.decode("utf-8", "replace")), pos
        if prefix == b":":
            return self._int(pos + 1)
        if prefix == b"$":
            size, pos = self._int(pos + 1)
            if size == -1:
                return None, pos
            if size < 0 or size > MAX_BULK_SIZE:
                raise ProtocolError(f"tamanho de bulk inválido: {size}")
            end = pos + size
            if end + 2 > len(self._buf):
                raise _Incomplete
            # o tamanho vem antes, então \r\n dentro do valor não confunde o parser
            if self._buf[end : end + 2] != CRLF:
                raise ProtocolError("bulk string sem \\r\\n no fim")
            return bytes(self._buf[pos:end]), end + 2
        if prefix == b"*":
            if depth >= MAX_DEPTH:
                raise ProtocolError("arrays aninhados demais")
            count, pos = self._int(pos + 1)
            if count == -1:
                return None, pos
            if count < 0:
                raise ProtocolError(f"tamanho de array inválido: {count}")
            items = []
            for _ in range(count):
                item, pos = self._parse(pos, depth + 1)
                items.append(item)
            return items, pos
        line, pos = self._line(pos)
        return line.split(), pos


def encode_simple(text: str) -> bytes:
    return b"+" + text.encode("utf-8") + CRLF


def encode_error(message: str) -> bytes:
    return b"-" + message.replace("\r", " ").replace("\n", " ").encode("utf-8") + CRLF


def encode_integer(value: int) -> bytes:
    return b":" + str(value).encode() + CRLF


def encode_bulk(value: bytes | None) -> bytes:
    if value is None:
        return b"$-1\r\n"
    return b"$" + str(len(value)).encode() + CRLF + value + CRLF


def encode_array(items: list[bytes] | None) -> bytes:
    if items is None:
        return b"*-1\r\n"
    return b"*" + str(len(items)).encode() + CRLF + b"".join(encode_bulk(i) for i in items)


def encode_command(*args: bytes | str | int | float) -> bytes:
    """Codifica um comando como array de bulk strings, do jeito que o redis-cli envia."""
    parts = []
    for arg in args:
        if isinstance(arg, bytes):
            parts.append(arg)
        else:
            parts.append(str(arg).encode("utf-8"))
    return encode_array(parts)
