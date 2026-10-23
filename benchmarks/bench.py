"""Benchmark do minikv: escritas, leituras, abertura, compactação e servidor.

Uso: python benchmarks/bench.py [--keys 100000] [--value-size 100]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import platform
import random
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

from minikv import Client, MiniKV
from minikv.server import start_server


def timed(fn) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def bench_writes(path: Path, n: int, value: bytes, sync: str) -> float:
    with MiniKV(path, sync=sync) as db:
        return n / timed(lambda: [db.set(f"key:{i}", value) for i in range(n)])


def bench_reads(path: Path, n: int) -> float:
    keys = [f"key:{random.randrange(n)}" for _ in range(n)]
    with MiniKV(path, sync="never") as db:
        return n / timed(lambda: [db.get(k) for k in keys])


def bench_open(path: Path) -> float:
    def open_close() -> None:
        MiniKV(path, sync="never").close()

    return timed(open_close)


def run_server(db: MiniKV, ready: threading.Event, holder: dict) -> None:
    loop = asyncio.new_event_loop()
    server = loop.run_until_complete(start_server(db, "127.0.0.1", 0))
    holder["port"] = server.sockets[0].getsockname()[1]
    holder["loop"] = loop
    ready.set()
    loop.run_forever()
    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


def bench_server(path: Path, n: int, value: bytes) -> tuple[float, float]:
    with MiniKV(path, sync="never") as db:
        ready, holder = threading.Event(), {}
        thread = threading.Thread(target=run_server, args=(db, ready, holder), daemon=True)
        thread.start()
        ready.wait()
        with Client("127.0.0.1", holder["port"]) as c:
            sequential = n / timed(lambda: [c.set(f"net:{i}", value) for i in range(n)])
            batch = [("SET", f"pipe:{i}", value) for i in range(n)]
            pipelined = n / timed(
                lambda: [c.pipeline(*batch[i : i + 100]) for i in range(0, n, 100)]
            )
        holder["loop"].call_soon_threadsafe(holder["loop"].stop)
        thread.join()
    return sequential, pipelined


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keys", type=int, default=100_000)
    parser.add_argument("--value-size", type=int, default=100)
    parser.add_argument("--fsync-writes", type=int, default=2_000)
    parser.add_argument("--net-ops", type=int, default=20_000)
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    random.seed(42)
    n = args.keys
    value = os.urandom(args.value_size // 2).hex().encode()
    base = Path(tempfile.mkdtemp(prefix="minikv-bench-"))
    rows: list[tuple[str, str]] = []
    try:
        db = base / "main"
        ops = bench_writes(db, n, value, "never")
        rows.append((f"escrita, sync=never ({n:,} chaves)", f"{ops:,.0f} ops/s"))
        ops = bench_writes(base / "interval", n, value, "interval")
        rows.append((f"escrita, sync=interval ({n:,} chaves)", f"{ops:,.0f} ops/s"))
        ops = bench_writes(base / "always", args.fsync_writes, value, "always")
        rows.append((f"escrita, sync=always ({args.fsync_writes:,} chaves)", f"{ops:,.0f} ops/s"))
        rows.append((f"leitura aleatória ({n:,} chaves)", f"{bench_reads(db, n):,.0f} ops/s"))
        rows.append((f"abertura varrendo o log ({n:,} chaves)", f"{bench_open(db) * 1000:,.0f} ms"))

        with MiniKV(db, sync="never") as store:
            for i in range(n):
                store.set(f"key:{i}", value)
            result = store.compact()
        rows.append(
            (
                f"compactação ({2 * n:,} registros, {n:,} vivos)",
                f"{result.seconds * 1000:,.0f} ms "
                f"({result.bytes_before / 2**20:.1f} -> {result.bytes_after / 2**20:.1f} MiB)",
            )
        )
        rows.append((f"abertura com hint ({n:,} chaves)", f"{bench_open(db) * 1000:,.0f} ms"))

        seq, pipe = bench_server(base / "net", args.net_ops, value)
        rows.append((f"SET pela rede, um por vez ({args.net_ops:,})", f"{seq:,.0f} ops/s"))
        rows.append((f"SET pela rede, pipeline de 100 ({args.net_ops:,})", f"{pipe:,.0f} ops/s"))
    finally:
        shutil.rmtree(base, ignore_errors=True)

    print(
        f"Python {platform.python_version()} em {platform.system()} {platform.release()}, ", end=""
    )
    print(f"{platform.processor() or platform.machine()}, valor de {args.value_size} bytes\n")
    print("| Medida | Resultado |")
    print("|---|---|")
    for name, result in rows:
        print(f"| {name} | {result} |")


if __name__ == "__main__":
    main()
