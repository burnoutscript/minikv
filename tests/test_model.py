"""Teste de propriedade: sequência aleatória de operações comparada com um dict."""

import random

import pytest

from minikv import MiniKV


@pytest.mark.parametrize("seed", [1, 7, 42, 2026])
def test_operacoes_aleatorias_batem_com_dict(db_path, clock, seed):
    rng = random.Random(seed)
    keys = [f"k{i}" for i in range(40)]
    model: dict[bytes, tuple[bytes, float | None]] = {}

    def live_model():
        return {k: v for k, (v, exp) in model.items() if exp is None or exp > clock.now}

    db = MiniKV(db_path, sync="never", max_file_size=2048, clock=clock)
    try:
        for _ in range(1500):
            op = rng.random()
            key = rng.choice(keys)
            kb = key.encode()
            if op < 0.45:
                value = rng.randbytes(rng.randint(0, 30))
                ttl = rng.choice([None, None, None, 1, 5])
                db.set(key, value, ttl=ttl)
                model[kb] = (value, None if ttl is None else clock.now + ttl)
            elif op < 0.65:
                assert db.delete(key) == (kb in live_model())
                model.pop(kb, None)
            elif op < 0.85:
                assert db.get(key) == live_model().get(kb)
            elif op < 0.9:
                clock.advance(rng.choice([0.5, 1, 3]))
            elif op < 0.95:
                db.compact()
            else:
                db.close()
                db = MiniKV(db_path, sync="never", max_file_size=2048, clock=clock)
        live = live_model()
        assert sorted(db.keys()) == sorted(live)
        assert {k: db.get(k) for k in live} == live
    finally:
        db.close()
