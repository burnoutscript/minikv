import threading

from minikv import MiniKV


def test_preserva_exatamente_os_dados_vivos_e_reduz_tamanho(db_path):
    expected = {}
    with MiniKV(db_path, sync="never", max_file_size=4096) as db:
        for round_ in range(5):
            for i in range(100):
                db.set(f"k{i}", f"v{round_}-{i}")
                expected[f"k{i}".encode()] = f"v{round_}-{i}".encode()
        for i in range(0, 100, 3):
            db.delete(f"k{i}")
            del expected[f"k{i}".encode()]
        files_before = db.stats()["data_files"]
        result = db.compact()
        assert result.files_before == files_before > 1
        assert result.bytes_after < result.bytes_before / 4
        assert result.keys == len(expected)
        assert {k: db.get(k) for k in db.keys()} == expected
    with MiniKV(db_path) as db:
        assert {k: db.get(k) for k in db.keys()} == expected


def test_remocao_continua_valendo_depois_de_compactar_e_reabrir(db_path):
    with MiniKV(db_path) as db:
        db.set("apagada", "1")
        db.set("viva", "2")
        db.delete("apagada")
        db.compact()
        assert db.get("apagada") is None
    with MiniKV(db_path) as db:
        assert db.get("apagada") is None
        assert db.keys() == [b"viva"]


def test_reabertura_usa_hint(db_path):
    with MiniKV(db_path) as db:
        for i in range(50):
            db.set(f"k{i}", str(i))
        db.compact()
    with MiniKV(db_path) as db:
        assert db.hint_files_loaded == 1
        assert db.get("k42") == b"42"


def test_hint_corrompido_cai_para_varredura(db_path):
    with MiniKV(db_path) as db:
        db.set("a", "1")
        db.compact()
    hint = next(db_path.glob("*.hint"))
    hint.write_bytes(hint.read_bytes()[:-1])
    with MiniKV(db_path) as db:
        assert db.hint_files_loaded == 0
        assert db.get("a") == b"1"


def test_compactar_duas_vezes_e_escrever_entre_elas(db_path):
    with MiniKV(db_path) as db:
        db.set("a", "1")
        db.compact()
        db.set("a", "2")
        db.set("b", "3")
        db.compact()
        db.set("c", "4")
    with MiniKV(db_path) as db:
        assert {k: db.get(k) for k in db.keys()} == {b"a": b"2", b"b": b"3", b"c": b"4"}
    assert len(list(db_path.glob("*.data"))) == 2


def test_escritas_durante_a_compactacao_nao_se_perdem(db_path):
    with MiniKV(db_path, sync="never") as db:
        for i in range(2000):
            db.set(f"k{i}", "antigo")
        stop = threading.Event()
        written = []

        def writer():
            i = 0
            while not stop.is_set() or i < 50:
                db.set(f"k{i % 2000}", f"novo{i}")
                written.append(i)
                i += 1

        t = threading.Thread(target=writer)
        t.start()
        db.compact()
        stop.set()
        t.join()
        latest = {}
        for i in written:
            latest[f"k{i % 2000}".encode()] = f"novo{i}".encode()
        for key, value in latest.items():
            assert db.get(key) == value
    with MiniKV(db_path) as db:
        for key, value in latest.items():
            assert db.get(key) == value
        assert len(db) == 2000
