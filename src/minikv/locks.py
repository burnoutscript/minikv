"""Primitivas de travamento: lock de leitores/escritor e lock do diretório."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


class StoreLockedError(RuntimeError):
    """Outro processo (ou outra instância) já está com o diretório aberto."""


class RWLock:
    """Vários leitores ou um escritor, com preferência para o escritor."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers = 0
        self._writer = False

    @contextmanager
    def read(self) -> Iterator[None]:
        with self._cond:
            while self._writer:
                self._cond.wait()
            self._readers += 1
        try:
            yield
        finally:
            with self._cond:
                self._readers -= 1
                if self._readers == 0:
                    self._cond.notify_all()

    @contextmanager
    def write(self) -> Iterator[None]:
        with self._cond:
            while self._writer:
                self._cond.wait()
            # marcar antes de esperar impede que novos leitores furem a fila
            self._writer = True
            while self._readers:
                self._cond.wait()
        try:
            yield
        finally:
            with self._cond:
                self._writer = False
                self._cond.notify_all()


class DirLock:
    """Lock exclusivo em um arquivo LOCK, via flock (POSIX) ou msvcrt (Windows)."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._file = None

    def acquire(self) -> None:
        f = open(self.path, "a+b")
        try:
            if os.name == "nt":
                import msvcrt

                f.seek(0)
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            f.close()
            raise StoreLockedError(f"{self.path.parent} já está aberto por outro processo") from exc
        self._file = f

    def release(self) -> None:
        if self._file is None:
            return
        if os.name == "nt":
            import msvcrt

            self._file.seek(0)
            msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
        self._file.close()
        self._file = None
