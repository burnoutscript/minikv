"""Servidor TCP asyncio que fala um subconjunto do protocolo Redis (RESP2)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import platform
import signal
import sys
import time
from collections.abc import Callable

from . import __version__
from .locks import StoreLockedError
from .resp import (
    NEED_MORE,
    ProtocolError,
    RespParser,
    encode_array,
    encode_bulk,
    encode_error,
    encode_integer,
    encode_simple,
)
from .store import SYNC_POLICIES, MiniKV, NotAnIntegerError

OK = encode_simple("OK")
NOT_INTEGER = encode_error("ERR value is not an integer or out of range")
SYNTAX_ERROR = encode_error("ERR syntax error")


class CommandError(Exception):
    """Erro que vira uma resposta -ERR para o cliente."""


def _int_arg(raw: bytes) -> int:
    try:
        return int(raw)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range") from None


class Commands:
    """Traduz comandos RESP em chamadas ao MiniKV."""

    def __init__(self, store: MiniKV) -> None:
        self.store = store
        self.started = time.time()
        self.clients = 0
        # aridade conta o nome do comando; negativa = no mínimo |n|, como no Redis
        self.table: dict[str, tuple[Callable[[list[bytes]], bytes], int]] = {
            "PING": (self.ping, -1),
            "ECHO": (self.echo, 2),
            "GET": (self.get, 2),
            "SET": (self.set, -3),
            "DEL": (self.delete, -2),
            "EXISTS": (self.exists, -2),
            "KEYS": (self.keys, 2),
            "DBSIZE": (self.dbsize, 1),
            "INCR": (self.incr, 2),
            "INCRBY": (self.incrby, 3),
            "DECR": (self.decr, 2),
            "EXPIRE": (self.expire, 3),
            "TTL": (self.ttl, 2),
            "PTTL": (self.pttl, 2),
            "INFO": (self.info, -1),
            "COMMAND": (self.command, -1),
            "SELECT": (self.select, 2),
            "HELLO": (self.hello, -1),
            "CLIENT": (self.client, -2),
        }

    async def execute(self, args: list[bytes]) -> bytes:
        """Executa um comando já parseado e devolve a resposta codificada."""
        name = args[0].decode("utf-8", "replace").upper()
        if name == "COMPACT":
            if len(args) != 1:
                return _arity_error(name)
            # compactação lê e reescreve arquivos inteiros; numa thread o loop
            # continua atendendo os outros clientes
            result = await asyncio.to_thread(self.store.compact)
            return encode_simple(
                f"OK {result.bytes_before} -> {result.bytes_after} bytes, {result.keys} keys"
            )
        spec = self.table.get(name)
        if spec is None:
            return encode_error(f"ERR unknown command '{name.lower()}'")
        handler, arity = spec
        if (arity > 0 and len(args) != arity) or (arity < 0 and len(args) < -arity):
            return _arity_error(name)
        try:
            return handler(args)
        except CommandError as exc:
            return encode_error(str(exc))
        except NotAnIntegerError:
            return NOT_INTEGER
        except ValueError as exc:
            return encode_error(f"ERR {exc}")

    def ping(self, args: list[bytes]) -> bytes:
        if len(args) > 2:
            return _arity_error("PING")
        return encode_bulk(args[1]) if len(args) == 2 else encode_simple("PONG")

    def echo(self, args: list[bytes]) -> bytes:
        return encode_bulk(args[1])

    def get(self, args: list[bytes]) -> bytes:
        return encode_bulk(self.store.get(args[1]))

    def set(self, args: list[bytes]) -> bytes:
        ttl: float | None = None
        options = [a.upper() for a in args[3:]]
        i = 0
        while i < len(options):
            if options[i] in (b"EX", b"PX") and i + 1 < len(options) and ttl is None:
                amount = _int_arg(args[3 + i + 1])
                if amount <= 0:
                    raise CommandError("ERR invalid expire time in 'set' command")
                ttl = amount if options[i] == b"EX" else amount / 1000
                i += 2
            else:
                return SYNTAX_ERROR
        self.store.set(args[1], args[2], ttl=ttl)
        return OK

    def delete(self, args: list[bytes]) -> bytes:
        return encode_integer(sum(self.store.delete(k) for k in args[1:]))

    def exists(self, args: list[bytes]) -> bytes:
        return encode_integer(sum(self.store.exists(k) for k in args[1:]))

    def keys(self, args: list[bytes]) -> bytes:
        return encode_array(sorted(self.store.keys(args[1])))

    def dbsize(self, args: list[bytes]) -> bytes:
        return encode_integer(len(self.store))

    def incr(self, args: list[bytes]) -> bytes:
        return encode_integer(self.store.incr(args[1]))

    def incrby(self, args: list[bytes]) -> bytes:
        return encode_integer(self.store.incr(args[1], _int_arg(args[2])))

    def decr(self, args: list[bytes]) -> bytes:
        return encode_integer(self.store.incr(args[1], -1))

    def expire(self, args: list[bytes]) -> bytes:
        return encode_integer(int(self.store.expire(args[1], _int_arg(args[2]))))

    def _ttl_ms(self, key: bytes) -> int:
        try:
            remaining = self.store.ttl(key)
        except KeyError:
            return -2
        return -1 if remaining is None else max(0, round(remaining * 1000))

    def ttl(self, args: list[bytes]) -> bytes:
        ms = self._ttl_ms(args[1])
        return encode_integer(ms if ms < 0 else (ms + 500) // 1000)

    def pttl(self, args: list[bytes]) -> bytes:
        return encode_integer(self._ttl_ms(args[1]))

    def info(self, args: list[bytes]) -> bytes:
        stats = self.store.stats()
        lines = [
            "# Server",
            f"minikv_version:{__version__}",
            f"python_version:{platform.python_version()}",
            f"uptime_in_seconds:{int(time.time() - self.started)}",
            "# Clients",
            f"connected_clients:{self.clients}",
            "# Storage",
            *(f"{name}:{value}" for name, value in stats.items() if name != "keys"),
            "# Keyspace",
            f"db0:keys={stats['keys']}",
        ]
        return encode_bulk(("\r\n".join(lines) + "\r\n").encode())

    def command(self, args: list[bytes]) -> bytes:
        # o redis-cli manda COMMAND DOCS ao conectar; lista vazia basta para ele seguir
        return encode_array([])

    def hello(self, args: list[bytes]) -> bytes:
        if len(args) > 1 and args[1] != b"2":
            # clientes novos pedem RESP3; com NOPROTO eles sabem que precisam usar RESP2
            return encode_error("NOPROTO this server only speaks RESP2")
        fields = [b"server", b"minikv", b"version", __version__.encode(), b"proto", b"2"]
        return encode_array(fields)

    def client(self, args: list[bytes]) -> bytes:
        # CLIENT SETNAME/SETINFO só identificam a conexão; não há o que guardar
        return OK

    def select(self, args: list[bytes]) -> bytes:
        if args[1] != b"0":
            return encode_error("ERR DB index is out of range")
        return OK


def _arity_error(name: str) -> bytes:
    return encode_error(f"ERR wrong number of arguments for '{name.lower()}' command")


async def handle_client(
    commands: Commands, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """Atende uma conexão; respostas de comandos em pipeline saem num único write."""
    parser = RespParser()
    commands.clients += 1
    try:
        closing = False
        while not closing:
            data = await reader.read(65536)
            if not data:
                break
            parser.feed(data)
            replies: list[bytes] = []
            while True:
                try:
                    request = parser.get()
                except ProtocolError as exc:
                    replies.append(encode_error(f"ERR Protocol error: {exc}"))
                    closing = True
                    break
                if request is NEED_MORE:
                    break
                if not isinstance(request, list) or not all(isinstance(a, bytes) for a in request):
                    replies.append(encode_error("ERR Protocol error: expected array of bulk"))
                    closing = True
                    break
                if not request:
                    continue
                if request[0].upper() == b"QUIT":
                    replies.append(OK)
                    closing = True
                    break
                replies.append(await commands.execute(request))
            if replies:
                writer.write(b"".join(replies))
                await writer.drain()
    except ConnectionError:
        pass
    finally:
        commands.clients -= 1
        writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await writer.wait_closed()


async def start_server(store: MiniKV, host: str = "127.0.0.1", port: int = 6380) -> asyncio.Server:
    """Abre o servidor; port=0 escolhe uma porta livre."""
    commands = Commands(store)
    return await asyncio.start_server(
        lambda r, w: handle_client(commands, r, w), host=host, port=port
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="minikv-server",
        description="Servidor minikv compatível com um subconjunto do protocolo Redis.",
        epilog="Exemplo: minikv-server --dir dados --port 6380, depois redis-cli -p 6380",
    )
    parser.add_argument("--dir", default="minikv-data", help="diretório dos dados")
    parser.add_argument("--host", default="127.0.0.1", help="endereço (padrão: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=6380, help="porta (padrão: 6380)")
    parser.add_argument(
        "--sync", choices=SYNC_POLICIES, default="interval", help="política de fsync"
    )
    parser.add_argument(
        "--sync-interval-ms", type=int, default=1000, help="intervalo do fsync em modo interval"
    )
    return parser


async def _serve(args: argparse.Namespace) -> None:
    with MiniKV(args.dir, sync=args.sync, sync_interval_ms=args.sync_interval_ms) as store:
        server = await start_server(store, args.host, args.port)
        address = server.sockets[0].getsockname()
        print(f"minikv {__version__} ouvindo em {address[0]}:{address[1]} (dados em {args.dir})")
        sys.stdout.flush()
        stop = asyncio.Event()
        # SIGTERM (kill, docker stop) fecha o store direito; no Windows não há
        # add_signal_handler e o Ctrl+C chega como KeyboardInterrupt
        with contextlib.suppress(NotImplementedError):
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
        async with server:
            await stop.wait()


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = build_parser().parse_args(argv)
    try:
        asyncio.run(_serve(args))
    except StoreLockedError as exc:
        print(f"erro: {exc}", file=sys.stderr)
        return 2
    except OSError as exc:
        print(f"erro: não foi possível abrir {args.host}:{args.port}: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("encerrado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
