import pytest

from minikv import MiniKV, NotAnIntegerError, StoreClosedError, StoreLockedError
from minikv.record import HEADER_SIZE


def data_files(path):
    return sorted(path.glob("*.data"))


def test_set_get_sobrescreve_e_remove(store):
    store.set("a", "1")
    store.set("a", "2")
    assert store.get("a") == b"2"
    assert store.delete("a") is True
    assert store.get("a") is None
    assert store.delete("a") is False


def test_chaves_str_e_bytes_sao_equivalentes(store):
    store.set("ação", b"\x00\xff")
    assert store.get("ação".encode()) == b"\x00\xff"
    assert "ação" in store
    assert len(store) == 1


def test_persiste_ao_reabrir(db_path):
    with MiniKV(db_path) as db:
        db.set("usuario:1", "victor")
        db.set("usuario:2", "ana")
        db.delete("usuario:2")
    with MiniKV(db_path) as db:
        assert db.get("usuario:1") == b"victor"
        assert db.get("usuario:2") is None
        assert db.keys() == [b"usuario:1"]


@pytest.mark.parametrize("policy", ["always", "interval", "never"])
def test_todas_as_politicas_de_sync_persistem(db_path, policy):
    with MiniKV(db_path, sync=policy, sync_interval_ms=10) as db:
        for i in range(20):
            db.set(f"k{i}", str(i))
    with MiniKV(db_path) as db:
        assert len(db) == 20


def test_politica_invalida(db_path):
    with pytest.raises(ValueError):
        MiniKV(db_path, sync="as-vezes")


def test_rotaciona_arquivos_quando_passa_do_limite(db_path):
    with MiniKV(db_path, max_file_size=200) as db:
        for i in range(30):
            db.set(f"chave{i}", "x" * 20)
        assert db.stats()["data_files"] > 1
    assert len(data_files(db_path)) > 1
    with MiniKV(db_path) as db:
        assert all(db.get(f"chave{i}") == b"x" * 20 for i in range(30))


def test_cauda_rasgada_e_truncada_ao_abrir(db_path):
    with MiniKV(db_path) as db:
        db.set("a", "1")
        db.set("b", "2")
        db.set("c", "valor-que-sera-cortado")
    path = data_files(db_path)[-1]
    size = path.stat().st_size
    with open(path, "r+b") as f:
        f.truncate(size - 5)
    with MiniKV(db_path) as db:
        assert db.get("a") == b"1"
        assert db.get("b") == b"2"
        assert db.get("c") is None
        assert db.recovered_bytes == HEADER_SIZE + 1 + len("valor-que-sera-cortado") - 5
        db.set("d", "depois")
    with MiniKV(db_path) as db:
        assert sorted(db.keys()) == [b"a", b"b", b"d"]
        assert db.recovered_bytes == 0


def test_byte_corrompido_no_ultimo_registro(db_path):
    with MiniKV(db_path) as db:
        db.set("a", "1")
        db.set("b", "2")
    path = data_files(db_path)[-1]
    data = bytearray(path.read_bytes())
    data[-1] ^= 0xFF
    path.write_bytes(bytes(data))
    with MiniKV(db_path) as db:
        assert db.get("a") == b"1"
        assert db.get("b") is None
    assert path.stat().st_size == HEADER_SIZE + 2


def test_lixo_no_fim_do_arquivo(db_path):
    with MiniKV(db_path) as db:
        db.set("a", "1")
    path = data_files(db_path)[-1]
    with open(path, "ab") as f:
        f.write(b"\x13\x37" * 7)
    with MiniKV(db_path) as db:
        assert db.get("a") == b"1"
        assert db.recovered_bytes == 14


def test_segunda_instancia_no_mesmo_diretorio_e_bloqueada(db_path):
    with MiniKV(db_path):
        with pytest.raises(StoreLockedError):
            MiniKV(db_path)
    with MiniKV(db_path):
        pass


def test_store_fechado_recusa_operacoes(db_path):
    db = MiniKV(db_path)
    db.close()
    db.close()
    with pytest.raises(StoreClosedError):
        db.get("a")
    with pytest.raises(StoreClosedError):
        db.set("a", "1")


def test_incr(store):
    assert store.incr("contador") == 1
    assert store.incr("contador", 10) == 11
    assert store.incr("contador", -20) == -9
    store.set("texto", "abc")
    with pytest.raises(NotAnIntegerError):
        store.incr("texto")
    store.set("espaco", " 1")
    with pytest.raises(NotAnIntegerError):
        store.incr("espaco")


def test_keys_com_padrao(store):
    for key in ["user:1", "user:2", "sessao:1", "user:10"]:
        store.set(key, "x")
    assert sorted(store.keys("user:?")) == [b"user:1", b"user:2"]
    assert sorted(store.keys("*:1*")) == [b"sessao:1", b"user:1", b"user:10"]
    assert store.keys("nada*") == []


def test_stats(store):
    store.set("a", "1")
    store.set("a", "2")
    stats = store.stats()
    assert stats["keys"] == 1
    assert stats["live_bytes"] == HEADER_SIZE + 2
    assert stats["disk_bytes"] == 2 * (HEADER_SIZE + 2)


def test_arquivos_tmp_de_compactacao_interrompida_sao_apagados(db_path):
    db_path.mkdir()
    (db_path / "000002.data.tmp").write_bytes(b"lixo")
    with MiniKV(db_path) as db:
        assert len(db) == 0
    assert not list(db_path.glob("*.tmp"))
