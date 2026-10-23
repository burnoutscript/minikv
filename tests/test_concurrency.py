import threading

from minikv import MiniKV


def run_threads(target, count):
    errors = []

    def wrapped(n):
        try:
            target(n)
        except Exception as exc:  # pragma: no cover - só para relatar no assert
            errors.append(exc)

    threads = [threading.Thread(target=wrapped, args=(n,)) for n in range(count)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []


def test_escritores_em_paralelo_nao_perdem_chaves(db_path):
    with MiniKV(db_path, sync="never", max_file_size=8192) as db:
        run_threads(lambda n: [db.set(f"t{n}:{i}", f"{n}-{i}") for i in range(300)], 8)
        assert len(db) == 8 * 300
    with MiniKV(db_path) as db:
        assert all(db.get(f"t{n}:{i}") == f"{n}-{i}".encode() for n in range(8) for i in range(300))


def test_incr_concorrente_e_atomico(store):
    run_threads(lambda n: [store.incr("contador") for _ in range(500)], 8)
    assert store.get("contador") == b"4000"


def test_leitores_veem_sempre_um_valor_completo(db_path):
    values = {b"a" * 100, b"b" * 100}
    with MiniKV(db_path, sync="never", max_file_size=4096) as db:
        db.set("k", b"a" * 100)
        stop = threading.Event()

        def writer(_):
            i = 0
            while not stop.is_set():
                db.set("k", b"ab"[i % 2 : i % 2 + 1] * 100)
                i += 1

        def reader(_):
            for _ in range(3000):
                assert db.get("k") in values

        w = threading.Thread(target=writer, args=(0,))
        w.start()
        try:
            run_threads(reader, 4)
        finally:
            stop.set()
            w.join()


def test_leituras_durante_compactacao(db_path):
    with MiniKV(db_path, sync="never", max_file_size=4096) as db:
        for i in range(500):
            db.set(f"k{i}", f"v{i}")
        stop = threading.Event()
        reads = [0]

        def reader(_):
            while not stop.is_set():
                for i in range(0, 500, 7):
                    assert db.get(f"k{i}") == f"v{i}".encode()
                reads[0] += 1

        threads = [threading.Thread(target=reader, args=(n,)) for n in range(3)]
        for t in threads:
            t.start()
        for _ in range(5):
            db.compact()
        stop.set()
        for t in threads:
            t.join()
        assert reads[0] > 0
