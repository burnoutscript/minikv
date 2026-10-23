"""Interface de linha de comando e REPL do minikv."""

from __future__ import annotations

import argparse
import shlex
import sys
from typing import TextIO

from . import __version__
from .locks import StoreLockedError
from .store import SYNC_POLICIES, MiniKV, NotAnIntegerError

EXIT_OK = 0
EXIT_NOT_FOUND = 1
EXIT_ERROR = 2

REPL_HELP = """\
comandos:
  set CHAVE VALOR [TTL]   grava (TTL em segundos, opcional)
  get CHAVE               lê
  del CHAVE...            remove
  incr CHAVE              soma 1
  ttl CHAVE               segundos até expirar
  keys [PADRÃO]           lista chaves (glob: user:*)
  compact                 compacta os arquivos
  stats                   estatísticas
  exit                    sai"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="minikv",
        description="Banco chave-valor persistente. Sem comando, abre o REPL.",
        epilog="Exemplo: minikv dados set usuario:1 victor",
    )
    parser.add_argument("db", help="diretório do banco (criado se não existir)")
    parser.add_argument(
        "--sync", choices=SYNC_POLICIES, default="always", help="política de fsync (padrão: always)"
    )
    parser.add_argument("--version", action="version", version=f"minikv {__version__}")
    sub = parser.add_subparsers(dest="command")
    p_set = sub.add_parser("set", help="grava uma chave")
    p_set.add_argument("key")
    p_set.add_argument("value")
    p_set.add_argument("--ttl", type=float, help="expira depois de N segundos")
    sub.add_parser("get", help="lê uma chave").add_argument("key")
    sub.add_parser("del", help="remove chaves").add_argument("keys", nargs="+")
    sub.add_parser("incr", help="soma 1 ao valor inteiro").add_argument("key")
    sub.add_parser("ttl", help="segundos até a chave expirar").add_argument("key")
    sub.add_parser("keys", help="lista chaves").add_argument("pattern", nargs="?")
    sub.add_parser("compact", help="compacta os arquivos de dados")
    sub.add_parser("stats", help="mostra estatísticas")
    sub.add_parser("repl", help="abre o REPL")
    return parser


def _text(value: bytes) -> str:
    return value.decode("utf-8", errors="replace")


def run_command(store: MiniKV, command: str, args: list[str], out: TextIO) -> int:
    """Executa um comando do CLI/REPL e escreve o resultado em out."""
    if command == "set":
        ttl = float(args[2]) if len(args) > 2 else None
        store.set(args[0], args[1], ttl=ttl)
        print("OK", file=out)
    elif command == "get":
        value = store.get(args[0])
        if value is None:
            print("(nil)", file=out)
            return EXIT_NOT_FOUND
        print(_text(value), file=out)
    elif command == "del":
        print(sum(store.delete(k) for k in args), file=out)
    elif command == "incr":
        print(store.incr(args[0]), file=out)
    elif command == "ttl":
        try:
            remaining = store.ttl(args[0])
        except KeyError:
            print("(nil)", file=out)
            return EXIT_NOT_FOUND
        print("sem expiração" if remaining is None else f"{remaining:.1f}s", file=out)
    elif command == "keys":
        for key in sorted(store.keys(args[0] if args else None)):
            print(_text(key), file=out)
    elif command == "compact":
        r = store.compact()
        print(
            f"{r.files_before} arquivo(s), {r.bytes_before} -> {r.bytes_after} bytes, "
            f"{r.keys} chaves, {r.seconds * 1000:.1f} ms",
            file=out,
        )
    elif command == "stats":
        for name, value in store.stats().items():
            print(f"{name:<18} {value}", file=out)
    else:
        raise ValueError(f"comando desconhecido: {command}")
    return EXIT_OK


_ARITY = {
    "set": (2, 3),
    "get": (1, 1),
    "del": (1, None),
    "incr": (1, 1),
    "ttl": (1, 1),
    "keys": (0, 1),
    "compact": (0, 0),
    "stats": (0, 0),
}


def repl(store: MiniKV, stdin: TextIO, out: TextIO) -> None:
    """Lê comandos linha a linha até exit ou fim da entrada."""
    interactive = stdin.isatty()
    if interactive:
        print(f"minikv {__version__} em {store.path} ('help' para ajuda)", file=out)
    while True:
        if interactive:
            out.write("minikv> ")
            out.flush()
        line = stdin.readline()
        if not line:
            break
        try:
            parts = shlex.split(line)
        except ValueError as exc:
            print(f"erro: {exc}", file=out)
            continue
        if not parts:
            continue
        command, args = parts[0].lower(), parts[1:]
        if command in ("exit", "quit"):
            break
        if command == "help":
            print(REPL_HELP, file=out)
            continue
        if command not in _ARITY:
            print(f"erro: comando desconhecido '{command}' (tente 'help')", file=out)
            continue
        low, high = _ARITY[command]
        if len(args) < low or (high is not None and len(args) > high):
            print(f"erro: número de argumentos errado para '{command}'", file=out)
            continue
        try:
            run_command(store, command, args, out)
        except (NotAnIntegerError, ValueError) as exc:
            print(f"erro: {exc}", file=out)


def _positional(args: argparse.Namespace) -> list[str]:
    if args.command == "set":
        return [args.key, args.value] + ([str(args.ttl)] if args.ttl is not None else [])
    if args.command == "del":
        return args.keys
    if args.command == "keys":
        return [args.pattern] if args.pattern else []
    if args.command in ("compact", "stats"):
        return []
    return [args.key]


def main(argv: list[str] | None = None) -> int:
    # console do Windows usa cp1252 e quebra os acentos
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = build_parser().parse_args(argv)
    try:
        store = MiniKV(args.db, sync=args.sync)
    except StoreLockedError as exc:
        print(f"erro: {exc}", file=sys.stderr)
        return EXIT_ERROR
    with store:
        if args.command in (None, "repl"):
            repl(store, sys.stdin, sys.stdout)
            return EXIT_OK
        try:
            return run_command(store, args.command, _positional(args), sys.stdout)
        except (NotAnIntegerError, ValueError) as exc:
            print(f"erro: {exc}", file=sys.stderr)
            return EXIT_ERROR
