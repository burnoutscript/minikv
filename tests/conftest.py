import asyncio
import threading

import pytest

from minikv import MiniKV
from minikv.server import start_server


class FakeClock:
    """Relógio controlado pelo teste, em segundos."""

    def __init__(self, now: float = 1_700_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "db"


@pytest.fixture
def store(db_path):
    with MiniKV(db_path, sync="never") as s:
        yield s


class ServerThread:
    """Servidor asyncio rodando num event loop próprio, numa thread separada."""

    def __init__(self, store: MiniKV) -> None:
        self.store = store
        self.port = 0
        self._ready = threading.Event()
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._server = self._loop.run_until_complete(start_server(self.store, "127.0.0.1", 0))
        self.port = self._server.sockets[0].getsockname()[1]
        self._ready.set()
        self._loop.run_forever()
        self._server.close()
        self._loop.run_until_complete(self._server.wait_closed())
        self._loop.close()

    def start(self) -> "ServerThread":
        self._thread.start()
        assert self._ready.wait(5)
        return self

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(5)


@pytest.fixture
def server(store):
    srv = ServerThread(store).start()
    yield srv
    srv.stop()
