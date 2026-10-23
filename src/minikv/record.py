"""Formato binário dos registros e das entradas do arquivo de hint."""

from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from typing import BinaryIO

# crc32 | timestamp_ms | expires_at_ms | flags | key_size | value_size
HEADER = struct.Struct("<IQQBII")
HEADER_SIZE = HEADER.size

# crc32 | expires_at_ms | value_pos | value_size | key_size
HINT_ENTRY = struct.Struct("<IQQII")
HINT_MAGIC = b"MKVH"
HINT_HEADER = struct.Struct("<4sQ")

FLAG_TOMBSTONE = 0x01
MAX_KEY_SIZE = 64 * 1024
MAX_VALUE_SIZE = 512 * 1024 * 1024


class CorruptRecordError(Exception):
    """Registro incompleto ou com CRC que não confere."""


@dataclass(frozen=True, slots=True)
class Record:
    """Um registro do log: escrita de valor ou tombstone de remoção."""

    key: bytes
    value: bytes
    timestamp: int
    expires_at: int = 0
    tombstone: bool = False

    @property
    def size(self) -> int:
        return HEADER_SIZE + len(self.key) + len(self.value)


def encode(record: Record) -> bytes:
    """Serializa o registro com o CRC32 cobrindo cabeçalho, chave e valor."""
    flags = FLAG_TOMBSTONE if record.tombstone else 0
    body = HEADER.pack(
        0, record.timestamp, record.expires_at, flags, len(record.key), len(record.value)
    )[4:]
    payload = body + record.key + record.value
    return struct.pack("<I", zlib.crc32(payload)) + payload


def decode(data: bytes) -> Record:
    """Desserializa um registro completo, validando tamanho e CRC."""
    if len(data) < HEADER_SIZE:
        raise CorruptRecordError("cabeçalho incompleto")
    crc, timestamp, expires_at, flags, key_size, value_size = HEADER.unpack_from(data)
    if key_size > MAX_KEY_SIZE or value_size > MAX_VALUE_SIZE:
        raise CorruptRecordError("tamanho de chave ou valor inválido")
    end = HEADER_SIZE + key_size + value_size
    if len(data) < end:
        raise CorruptRecordError("registro incompleto")
    if zlib.crc32(data[4:end]) != crc:
        raise CorruptRecordError("CRC não confere")
    key = bytes(data[HEADER_SIZE : HEADER_SIZE + key_size])
    value = bytes(data[HEADER_SIZE + key_size : end])
    return Record(key, value, timestamp, expires_at, bool(flags & FLAG_TOMBSTONE))


def read_record(f: BinaryIO) -> Record | None:
    """Lê o próximo registro do arquivo; None no fim limpo do arquivo."""
    header = f.read(HEADER_SIZE)
    if not header:
        return None
    if len(header) < HEADER_SIZE:
        raise CorruptRecordError("cabeçalho incompleto")
    _, _, _, _, key_size, value_size = HEADER.unpack(header)
    # tamanhos absurdos indicam lixo no cabeçalho; sem esse teste um byte trocado
    # faria o scanner tentar ler gigabytes
    if key_size > MAX_KEY_SIZE or value_size > MAX_VALUE_SIZE:
        raise CorruptRecordError("tamanho de chave ou valor inválido")
    payload = f.read(key_size + value_size)
    return decode(header + payload)


def encode_hint(key: bytes, expires_at: int, value_pos: int, value_size: int) -> bytes:
    """Serializa uma entrada do arquivo de hint."""
    body = HINT_ENTRY.pack(0, expires_at, value_pos, value_size, len(key))[4:] + key
    return struct.pack("<I", zlib.crc32(body)) + body


def decode_hints(data: bytes) -> tuple[int, list[tuple[bytes, int, int, int]]]:
    """Lê um arquivo de hint inteiro: (tamanho do arquivo de dados, entradas)."""
    if len(data) < HINT_HEADER.size:
        raise CorruptRecordError("hint sem cabeçalho")
    magic, data_size = HINT_HEADER.unpack_from(data)
    if magic != HINT_MAGIC:
        raise CorruptRecordError("hint com assinatura inválida")
    entries = []
    pos = HINT_HEADER.size
    while pos < len(data):
        if pos + HINT_ENTRY.size > len(data):
            raise CorruptRecordError("entrada de hint incompleta")
        crc, expires_at, value_pos, value_size, key_size = HINT_ENTRY.unpack_from(data, pos)
        end = pos + HINT_ENTRY.size + key_size
        if end > len(data) or zlib.crc32(data[pos + 4 : end]) != crc:
            raise CorruptRecordError("entrada de hint corrompida")
        key = bytes(data[pos + HINT_ENTRY.size : end])
        entries.append((key, expires_at, value_pos, value_size))
        pos = end
    return data_size, entries
