import io

import pytest

from minikv import record as rec
from minikv.record import CorruptRecordError, Record


def test_encode_decode_ida_e_volta():
    original = Record(b"usuario:1", b"victor", timestamp=123, expires_at=456)
    data = rec.encode(original)
    assert len(data) == original.size == rec.HEADER_SIZE + 9 + 6
    assert rec.decode(data) == original


def test_tombstone_ida_e_volta():
    tomb = Record(b"k", b"", timestamp=1, tombstone=True)
    assert rec.decode(rec.encode(tomb)).tombstone is True


def test_valor_vazio_e_bytes_arbitrarios():
    value = bytes(range(256)) + b"\r\n\x00"
    original = Record(b"", value, timestamp=0)
    assert rec.decode(rec.encode(original)).value == value


@pytest.mark.parametrize("position", [0, 5, rec.HEADER_SIZE, -1])
def test_crc_detecta_qualquer_byte_trocado(position):
    data = bytearray(rec.encode(Record(b"chave", b"valor", timestamp=7)))
    data[position] ^= 0xFF
    with pytest.raises(CorruptRecordError):
        rec.decode(bytes(data))


@pytest.mark.parametrize("cut", [1, rec.HEADER_SIZE - 1, rec.HEADER_SIZE + 2])
def test_registro_truncado_e_corrupcao(cut):
    data = rec.encode(Record(b"chave", b"valor", timestamp=7))
    with pytest.raises(CorruptRecordError):
        rec.read_record(io.BytesIO(data[:cut]))


def test_read_record_sequencial_ate_o_fim():
    records = [Record(f"k{i}".encode(), b"v" * i, timestamp=i) for i in range(5)]
    f = io.BytesIO(b"".join(rec.encode(r) for r in records))
    read = []
    while (r := rec.read_record(f)) is not None:
        read.append(r)
    assert read == records


def test_tamanho_absurdo_no_cabecalho_nao_tenta_ler():
    header = rec.HEADER.pack(0, 0, 0, 0, rec.MAX_KEY_SIZE + 1, 0)
    with pytest.raises(CorruptRecordError, match="tamanho"):
        rec.read_record(io.BytesIO(header))


def test_hint_ida_e_volta_e_corrupcao():
    body = rec.encode_hint(b"a", 0, 29, 3) + rec.encode_hint(b"bb", 99, 70, 0)
    data = rec.HINT_HEADER.pack(rec.HINT_MAGIC, 1000) + body
    assert rec.decode_hints(data) == (1000, [(b"a", 0, 29, 3), (b"bb", 99, 70, 0)])
    damaged = bytearray(data)
    damaged[-1] ^= 1
    with pytest.raises(CorruptRecordError):
        rec.decode_hints(bytes(damaged))
