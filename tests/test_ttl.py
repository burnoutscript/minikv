import pytest

from minikv import MiniKV


@pytest.fixture
def db(db_path, clock):
    with MiniKV(db_path, sync="never", clock=clock) as s:
        yield s


def test_expira_na_leitura(db, clock):
    db.set("sessao", "abc", ttl=10)
    clock.advance(9.9)
    assert db.get("sessao") == b"abc"
    clock.advance(0.1)
    assert db.get("sessao") is None
    assert "sessao" not in db
    assert db.keys() == []


def test_ttl_restante_e_chave_sem_ttl(db, clock):
    db.set("a", "1", ttl=30)
    db.set("b", "2")
    clock.advance(10)
    assert db.ttl("a") == pytest.approx(20)
    assert db.ttl("b") is None
    with pytest.raises(KeyError):
        db.ttl("nao-existe")


def test_set_sem_ttl_remove_expiracao(db, clock):
    db.set("a", "1", ttl=5)
    db.set("a", "2")
    clock.advance(100)
    assert db.get("a") == b"2"


def test_expire_em_chave_existente_e_inexistente(db, clock):
    db.set("a", "1")
    assert db.expire("a", 5) is True
    assert db.expire("fantasma", 5) is False
    clock.advance(5)
    assert db.get("a") is None


def test_expire_nao_positivo_remove(db):
    db.set("a", "1")
    assert db.expire("a", 0) is True
    assert db.get("a") is None


def test_ttl_invalido(db):
    with pytest.raises(ValueError):
        db.set("a", "1", ttl=0)


def test_incr_preserva_ttl(db, clock):
    db.set("n", "1", ttl=10)
    assert db.incr("n") == 2
    clock.advance(10)
    assert db.get("n") is None


def test_expiracao_sobrevive_a_reabertura(db_path, clock):
    with MiniKV(db_path, clock=clock) as db:
        db.set("curta", "x", ttl=1)
        db.set("longa", "y", ttl=1000)
    clock.advance(2)
    with MiniKV(db_path, clock=clock) as db:
        assert db.get("curta") is None
        assert db.get("longa") == b"y"
        assert db.ttl("longa") == pytest.approx(998)


def test_compactacao_descarta_expiradas(db, clock):
    db.set("curta", "x" * 100, ttl=1)
    db.set("longa", "y", ttl=1000)
    clock.advance(2)
    result = db.compact()
    assert result.keys == 1
    assert db.keys() == [b"longa"]
    assert db.ttl("longa") == pytest.approx(998)
