"""Motor de armazenamento log-structured no estilo Bitcask."""

from __future__ import annotations

import os
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Literal, NamedTuple

from . import record as rec
from .locks import DirLock, RWLock

SyncPolicy = Literal["always", "interval", "never"]
SYNC_POLICIES = ("always", "interval", "never")
Key = bytes | str
Value = bytes | str

DATA_SUFFIX = ".data"
HINT_SUFFIX = ".hint"
TMP_SUFFIX = ".tmp"
_INTEGER = re.compile(rb"-?(0|[1-9][0-9]*)")
_HAS_PREAD = hasattr(os, "pread")


class StoreClosedError(RuntimeError):
    """Operação em um store já fechado."""


class NotAnIntegerError(ValueError):
    """INCR em valor que não é inteiro."""


class Entry(NamedTuple):
    """Posição do valor mais recente de uma chave."""

    file_id: int
    value_pos: int
    value_size: int
    expires_at: int


@dataclass(frozen=True)
class CompactionResult:
    """Resumo de uma compactação."""

    files_before: int
    bytes_before: int
    bytes_after: int
    keys: int
    seconds: float


def _to_bytes(data: Key) -> bytes:
    return data.encode("utf-8") if isinstance(data, str) else bytes(data)


def _data_name(file_id: int) -> str:
    return f"{file_id:06d}{DATA_SUFFIX}"


def _hint_name(file_id: int) -> str:
    return f"{file_id:06d}{HINT_SUFFIX}"


def _fsync_dir(path: Path) -> None:
    # no POSIX o rename só é durável depois do fsync do diretório; o Windows não
    # permite abrir diretório assim e o NTFS já registra o rename no journal
    if os.name == "nt":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_all(f, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = f.write(view)
        view = view[written:]


class _DataFile:
    """Handle de leitura de um arquivo de dados."""

    def __init__(self, file_id: int, path: Path) -> None:
        self.file_id = file_id
        self.path = path
        self._f = open(path, "rb", buffering=0)
        self._lock = threading.Lock()

    def read(self, pos: int, size: int) -> bytes:
        if _HAS_PREAD:
            chunks = []
            while size > 0:
                chunk = os.pread(self._f.fileno(), size, pos)
                if not chunk:
                    raise OSError(f"fim inesperado de {self.path}")
                chunks.append(chunk)
                pos += len(chunk)
                size -= len(chunk)
            return b"".join(chunks)
        # sem pread (Windows) o par seek+read precisa ser atômico
        with self._lock:
            self._f.seek(pos)
            data = self._f.read(size)
        if len(data) != size:
            raise OSError(f"fim inesperado de {self.path}")
        return data

    def close(self) -> None:
        self._f.close()


class MiniKV:
    """Store chave-valor persistente e thread-safe.

    Escritas são serializadas por um único lock e só acrescentam bytes ao arquivo ativo.
    Leituras não pegam esse lock: o índice aponta para offsets que nunca mudam, então basta
    um lock compartilhado que só a troca de arquivos da compactação pega de forma exclusiva.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        sync: SyncPolicy = "interval",
        sync_interval_ms: int = 1000,
        max_file_size: int = 64 * 1024 * 1024,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if sync not in SYNC_POLICIES:
            raise ValueError(f"política de sync inválida: {sync!r}")
        if sync_interval_ms <= 0:
            raise ValueError("sync_interval_ms precisa ser positivo")
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.sync_policy: SyncPolicy = sync
        self.max_file_size = max_file_size
        self._clock = clock
        self._sync_interval = sync_interval_ms / 1000

        self._write_lock = threading.Lock()
        self._compact_lock = threading.Lock()
        self._files_lock = RWLock()
        self._index: dict[bytes, Entry] = {}
        self._files: dict[int, _DataFile] = {}
        self._active_id = 0
        self._active_size = 0
        self._writer = None
        self._dirty = False
        self._closed = False
        self.recovered_bytes = 0
        self.hint_files_loaded = 0

        self._dir_lock = DirLock(self.path / "LOCK")
        self._dir_lock.acquire()
        try:
            self._load()
        except BaseException:
            self._close_files()
            self._dir_lock.release()
            raise

        self._stop = threading.Event()
        self._sync_thread: threading.Thread | None = None
        if sync == "interval":
            self._sync_thread = threading.Thread(
                target=self._sync_loop, name="minikv-sync", daemon=True
            )
            self._sync_thread.start()

    def __enter__(self) -> MiniKV:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _now_ms(self) -> int:
        return int(self._clock() * 1000)

    def _is_expired(self, entry: Entry, now_ms: int | None = None) -> bool:
        if not entry.expires_at:
            return False
        return entry.expires_at <= (self._now_ms() if now_ms is None else now_ms)

    def _check_open(self) -> None:
        if self._closed:
            raise StoreClosedError("store fechado")

    def _load(self) -> None:
        for tmp in self.path.glob(f"*{TMP_SUFFIX}"):
            tmp.unlink()
        ids = sorted(int(p.stem) for p in self.path.glob(f"*{DATA_SUFFIX}") if p.stem.isdigit())
        now = self._now_ms()
        for file_id in ids:
            if not self._load_hint(file_id, now):
                self._scan(file_id, now)
            self._files[file_id] = _DataFile(file_id, self.path / _data_name(file_id))

        last = ids[-1] if ids else 0
        # arquivo com hint é resultado de compactação e não pode crescer, senão o hint
        # deixaria de descrever o arquivo inteiro
        if not ids or (self.path / _hint_name(last)).exists():
            self._open_active(last + 1)
        else:
            self._open_active(last)

    def _apply(self, record: rec.Record, file_id: int, offset: int, now: int) -> None:
        if record.tombstone or (record.expires_at and record.expires_at <= now):
            self._index.pop(record.key, None)
            return
        value_pos = offset + rec.HEADER_SIZE + len(record.key)
        self._index[record.key] = Entry(file_id, value_pos, len(record.value), record.expires_at)

    def _scan(self, file_id: int, now: int) -> None:
        path = self.path / _data_name(file_id)
        good_end = 0
        with open(path, "rb") as f:
            while True:
                offset = f.tell()
                try:
                    record = rec.read_record(f)
                except rec.CorruptRecordError:
                    break
                if record is None:
                    break
                self._apply(record, file_id, offset, now)
                good_end = f.tell()
        size = path.stat().st_size
        if good_end < size:
            # tudo antes de good_end passou no CRC; o que sobra é uma escrita
            # interrompida que nunca foi confirmada ao cliente
            with open(path, "r+b") as f:
                f.truncate(good_end)
                f.flush()
                os.fsync(f.fileno())
            self.recovered_bytes += size - good_end

    def _load_hint(self, file_id: int, now: int) -> bool:
        hint_path = self.path / _hint_name(file_id)
        if not hint_path.exists():
            return False
        try:
            data_size, entries = rec.decode_hints(hint_path.read_bytes())
        except rec.CorruptRecordError:
            return False
        if data_size != (self.path / _data_name(file_id)).stat().st_size:
            return False
        for key, expires_at, value_pos, value_size in entries:
            if expires_at and expires_at <= now:
                self._index.pop(key, None)
            else:
                self._index[key] = Entry(file_id, value_pos, value_size, expires_at)
        self.hint_files_loaded += 1
        return True

    def _open_active(self, file_id: int) -> None:
        path = self.path / _data_name(file_id)
        self._writer = open(path, "ab", buffering=0)
        self._active_id = file_id
        self._active_size = path.stat().st_size
        if file_id not in self._files:
            self._files[file_id] = _DataFile(file_id, path)

    def _rotate(self) -> None:
        assert self._writer is not None
        os.fsync(self._writer.fileno())
        self._writer.close()
        self._open_active(self._active_id + 1)

    def _append(self, record: rec.Record) -> Entry:
        data = rec.encode(record)
        if self._active_size and self._active_size + len(data) > self.max_file_size:
            self._rotate()
        assert self._writer is not None
        offset = self._active_size
        _write_all(self._writer, data)
        self._active_size += len(data)
        if self.sync_policy == "always":
            os.fsync(self._writer.fileno())
        else:
            self._dirty = True
        return Entry(
            self._active_id,
            offset + rec.HEADER_SIZE + len(record.key),
            len(record.value),
            record.expires_at,
        )

    def _put_locked(self, key: bytes, value: bytes, expires_at: int) -> None:
        record = rec.Record(key, value, self._now_ms(), expires_at)
        self._index[key] = self._append(record)

    def _read(self, entry: Entry) -> bytes:
        return self._files[entry.file_id].read(entry.value_pos, entry.value_size)

    def _live_entry(self, key: bytes) -> Entry | None:
        entry = self._index.get(key)
        if entry is None or self._is_expired(entry):
            return None
        return entry

    def _evict_if_unchanged(self, key: bytes, entry: Entry) -> None:
        with self._write_lock:
            if self._index.get(key) is entry:
                del self._index[key]

    def get(self, key: Key) -> bytes | None:
        """Valor atual da chave, ou None se não existe ou expirou."""
        k = _to_bytes(key)
        with self._files_lock.read():
            self._check_open()
            entry = self._index.get(k)
            if entry is None:
                return None
            if not self._is_expired(entry):
                return self._read(entry)
        self._evict_if_unchanged(k, entry)
        return None

    def set(self, key: Key, value: Value, ttl: float | None = None) -> None:
        """Grava a chave; com ttl (segundos) ela expira depois desse tempo."""
        if ttl is not None and ttl <= 0:
            raise ValueError("ttl precisa ser positivo")
        k, v = _to_bytes(key), _to_bytes(value)
        if len(k) > rec.MAX_KEY_SIZE or len(v) > rec.MAX_VALUE_SIZE:
            raise ValueError("chave ou valor grande demais")
        with self._write_lock:
            self._check_open()
            expires_at = self._now_ms() + int(ttl * 1000) if ttl is not None else 0
            self._put_locked(k, v, expires_at)

    def delete(self, key: Key) -> bool:
        """Remove a chave gravando um tombstone; False se ela não existia."""
        k = _to_bytes(key)
        with self._write_lock:
            self._check_open()
            entry = self._index.pop(k, None)
            if entry is None or self._is_expired(entry):
                return False
            self._append(rec.Record(k, b"", self._now_ms(), tombstone=True))
            return True

    def exists(self, key: Key) -> bool:
        """Se a chave existe e não expirou."""
        self._check_open()
        return self._live_entry(_to_bytes(key)) is not None

    __contains__ = exists

    def __len__(self) -> int:
        return len(self.keys())

    def keys(self, pattern: str | bytes | None = None) -> list[bytes]:
        """Chaves vivas, opcionalmente filtradas por padrão glob (fnmatch)."""
        with self._write_lock:
            self._check_open()
            items = list(self._index.items())
        now = self._now_ms()
        pat = _to_bytes(pattern) if pattern is not None else None
        return [
            k
            for k, entry in items
            if not self._is_expired(entry, now) and (pat is None or fnmatchcase(k, pat))
        ]

    def incr(self, key: Key, amount: int = 1) -> int:
        """Soma amount ao inteiro guardado na chave (0 se não existe), mantendo o TTL."""
        k = _to_bytes(key)
        with self._write_lock:
            self._check_open()
            entry = self._live_entry(k)
            current = 0
            expires_at = 0
            if entry is not None:
                raw = self._read(entry)
                if not _INTEGER.fullmatch(raw):
                    raise NotAnIntegerError("valor não é um inteiro")
                current = int(raw)
                expires_at = entry.expires_at
            result = current + amount
            self._put_locked(k, str(result).encode(), expires_at)
            return result

    def expire(self, key: Key, seconds: float) -> bool:
        """Define o TTL de uma chave existente; seconds <= 0 remove a chave."""
        k = _to_bytes(key)
        if seconds <= 0:
            return self.delete(k)
        with self._write_lock:
            self._check_open()
            entry = self._live_entry(k)
            if entry is None:
                return False
            self._put_locked(k, self._read(entry), self._now_ms() + int(seconds * 1000))
            return True

    def ttl(self, key: Key) -> float | None:
        """Segundos até expirar, None se não tem TTL; KeyError se a chave não existe."""
        self._check_open()
        k = _to_bytes(key)
        entry = self._live_entry(k)
        if entry is None:
            raise KeyError(key)
        if not entry.expires_at:
            return None
        return (entry.expires_at - self._now_ms()) / 1000

    def sync(self) -> None:
        """Força o fsync do arquivo ativo."""
        with self._write_lock:
            self._check_open()
            self._sync_locked()

    def _sync_locked(self) -> None:
        if self._writer is not None and self._dirty:
            os.fsync(self._writer.fileno())
            self._dirty = False

    def _sync_loop(self) -> None:
        while not self._stop.wait(self._sync_interval):
            with self._write_lock:
                if self._closed:
                    return
                self._sync_locked()

    def _data_files(self) -> list[Path]:
        return [self.path / _data_name(fid) for fid in sorted(self._files)]

    def stats(self) -> dict[str, int | str]:
        """Números de diagnóstico: chaves, arquivos e bytes em disco."""
        with self._write_lock:
            self._check_open()
            items = list(self._index.items())
            disk = sum(p.stat().st_size for p in self._data_files())
            files = len(self._files)
            active = self._active_id
        now = self._now_ms()
        live = [(k, e) for k, e in items if not self._is_expired(e, now)]
        return {
            "keys": len(live),
            "data_files": files,
            "active_file": active,
            "disk_bytes": disk,
            "live_bytes": sum(rec.HEADER_SIZE + len(k) + e.value_size for k, e in live),
            "recovered_bytes": self.recovered_bytes,
            "hint_files_loaded": self.hint_files_loaded,
            "sync_policy": self.sync_policy,
        }

    def compact(self) -> CompactionResult:
        """Reescreve só os registros vivos num arquivo novo e apaga os antigos."""
        with self._compact_lock:
            started = time.perf_counter()
            with self._write_lock:
                self._check_open()
                old_ids = sorted(self._files)
                bytes_before = sum(p.stat().st_size for p in self._data_files())
                merged_id = self._active_id + 1
                # o arquivo compactado fica entre os antigos e o novo ativo, então a
                # ordem de ids continua sendo a ordem cronológica na hora de reabrir
                self._writer.close()
                self._open_active(self._active_id + 2)
                snapshot = dict(self._index)

            new_entries = self._write_merged(merged_id, snapshot)
            merged_file = _DataFile(merged_id, self.path / _data_name(merged_id))

            with self._write_lock, self._files_lock.write():
                if self._closed:
                    merged_file.close()
                    raise StoreClosedError("store fechado durante a compactação")
                for key, old in snapshot.items():
                    if self._index.get(key) is not old:
                        continue
                    if key in new_entries:
                        self._index[key] = new_entries[key]
                    else:
                        del self._index[key]
                self._files[merged_id] = merged_file
                # em ordem crescente: se cair no meio, um tombstone nunca some antes do
                # registro que ele apaga, então nenhuma chave removida ressuscita
                for file_id in old_ids:
                    self._files.pop(file_id).close()
                    (self.path / _data_name(file_id)).unlink()
                    (self.path / _hint_name(file_id)).unlink(missing_ok=True)
                bytes_after = sum(p.stat().st_size for p in self._data_files())

            _fsync_dir(self.path)
            return CompactionResult(
                files_before=len(old_ids),
                bytes_before=bytes_before,
                bytes_after=bytes_after,
                keys=len(new_entries),
                seconds=time.perf_counter() - started,
            )

    def _write_merged(self, merged_id: int, snapshot: dict[bytes, Entry]) -> dict[bytes, Entry]:
        data_path = self.path / _data_name(merged_id)
        hint_path = self.path / _hint_name(merged_id)
        data_tmp = data_path.with_name(data_path.name + TMP_SUFFIX)
        hint_tmp = hint_path.with_name(hint_path.name + TMP_SUFFIX)
        now = self._now_ms()
        new_entries: dict[bytes, Entry] = {}
        hints: list[bytes] = []
        offset = 0
        with open(data_tmp, "wb") as out:
            for key, entry in snapshot.items():
                if self._is_expired(entry, now):
                    continue
                # arquivos antigos não mudam mais e só esta compactação os fecha
                value = self._read(entry)
                data = rec.encode(rec.Record(key, value, now, entry.expires_at))
                out.write(data)
                value_pos = offset + rec.HEADER_SIZE + len(key)
                new_entries[key] = Entry(merged_id, value_pos, len(value), entry.expires_at)
                hints.append(rec.encode_hint(key, entry.expires_at, value_pos, len(value)))
                offset += len(data)
            out.flush()
            os.fsync(out.fileno())
        with open(hint_tmp, "wb") as out:
            out.write(rec.HINT_HEADER.pack(rec.HINT_MAGIC, offset))
            out.write(b"".join(hints))
            out.flush()
            os.fsync(out.fileno())
        # fsync antes do replace: sem ele o rename pode chegar ao disco antes dos dados
        # e uma queda deixaria um arquivo com o nome certo e conteúdo vazio
        os.replace(data_tmp, data_path)
        os.replace(hint_tmp, hint_path)
        return new_entries

    def _close_files(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
        for f in self._files.values():
            f.close()
        self._files.clear()

    def close(self) -> None:
        """Sincroniza o arquivo ativo e libera arquivos e o lock do diretório."""
        if self._closed:
            return
        self._stop.set()
        with self._write_lock, self._files_lock.write():
            if self._closed:
                return
            if self._writer is not None and self.sync_policy != "never":
                os.fsync(self._writer.fileno())
            self._close_files()
            self._closed = True
            self._dir_lock.release()
        if self._sync_thread is not None:
            self._sync_thread.join()
