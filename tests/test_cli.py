import io

from minikv import MiniKV
from minikv.cli import EXIT_ERROR, EXIT_NOT_FOUND, EXIT_OK, main, repl


def test_set_get_del(db_path, capsys):
    assert main([str(db_path), "set", "nome", "Victor"]) == EXIT_OK
    assert main([str(db_path), "get", "nome"]) == EXIT_OK
    assert capsys.readouterr().out.splitlines() == ["OK", "Victor"]
    assert main([str(db_path), "del", "nome", "outra"]) == EXIT_OK
    assert capsys.readouterr().out.strip() == "1"
    assert main([str(db_path), "get", "nome"]) == EXIT_NOT_FOUND


def test_set_com_ttl_e_ttl(db_path, capsys):
    main([str(db_path), "set", "s", "x", "--ttl", "60"])
    capsys.readouterr()
    main([str(db_path), "ttl", "s"])
    assert capsys.readouterr().out.strip().endswith("s")


def test_compact_stats_keys(db_path, capsys):
    for i in range(5):
        main([str(db_path), "set", "k", str(i)])
    main([str(db_path), "set", "outra", "x"])
    capsys.readouterr()
    assert main([str(db_path), "compact"]) == EXIT_OK
    assert "2 chaves" in capsys.readouterr().out
    main([str(db_path), "stats"])
    out = capsys.readouterr().out
    assert "keys" in out and "disk_bytes" in out
    main([str(db_path), "keys", "o*"])
    assert capsys.readouterr().out.strip() == "outra"


def test_incr_em_texto_e_erro(db_path, capsys):
    main([str(db_path), "set", "t", "abc"])
    assert main([str(db_path), "incr", "t"]) == EXIT_ERROR


def test_diretorio_em_uso_e_erro(db_path):
    with MiniKV(db_path):
        assert main([str(db_path), "get", "a"]) == EXIT_ERROR


def test_repl(db_path):
    commands = (
        "set a 1\nincr a\nget a\nset 'com espaço' \"olá mundo\"\nkeys\nfoo\nget\nexit\nget a\n"
    )
    out = io.StringIO()
    with MiniKV(db_path) as db:
        repl(db, io.StringIO(commands), out)
    lines = out.getvalue().splitlines()
    assert lines[:5] == ["OK", "2", "2", "OK", "a"]
    assert lines[5] == "com espaço"
    assert "comando desconhecido" in lines[6]
    assert "argumentos" in lines[7]
    assert len(lines) == 8
