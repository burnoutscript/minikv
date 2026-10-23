import socket

import pytest

from minikv import Client, RespError
from minikv.resp import RespParser, encode_command


@pytest.fixture
def client(server):
    with Client("127.0.0.1", server.port) as c:
        yield c


def test_ping_e_echo(client):
    assert client.ping()
    assert client.execute("PING", "oi") == b"oi"
    assert client.execute("ECHO", "olá") == "olá".encode()


def test_set_get_del_exists(client):
    assert client.set("a", "1")
    assert client.get("a") == b"1"
    assert client.get("nada") is None
    client.set("b", "2")
    assert client.execute("EXISTS", "a", "b", "c") == 2
    assert client.delete("a", "c") == 1
    assert client.execute("DBSIZE") == 1


def test_valor_binario_com_crlf(client):
    value = b"\x00linha\r\noutra\r\n"
    client.set("bin", value)
    assert client.get("bin") == value


def test_set_com_ex_e_ttl(client):
    client.set("sessao", "x", ex=100)
    assert 99 <= client.execute("TTL", "sessao") <= 100
    assert 99_000 <= client.execute("PTTL", "sessao") <= 100_000
    assert client.execute("TTL", "a") == -2
    client.set("a", "1")
    assert client.execute("TTL", "a") == -1
    assert client.execute("EXPIRE", "a", 50) == 1
    assert client.execute("EXPIRE", "nada", 50) == 0
    assert client.execute("SET", "p", "1", "PX", 5000) == "OK"
    assert 4 <= client.execute("TTL", "p") <= 5


@pytest.mark.parametrize(
    "args, message",
    [
        (("SET", "a", "1", "EX", "abc"), "not an integer"),
        (("SET", "a", "1", "EX", "0"), "invalid expire time"),
        (("SET", "a", "1", "NX"), "syntax error"),
        (("GET",), "wrong number of arguments for 'get'"),
        (("PING", "a", "b"), "wrong number of arguments"),
        (("FLUSHALL",), "unknown command 'flushall'"),
        (("SELECT", "1"), "out of range"),
    ],
)
def test_erros(client, args, message):
    with pytest.raises(RespError, match=message):
        client.execute(*args)


def test_incr(client):
    assert client.execute("INCR", "n") == 1
    assert client.execute("INCRBY", "n", 41) == 42
    assert client.execute("DECR", "n") == 41
    client.set("t", "texto")
    with pytest.raises(RespError, match="not an integer"):
        client.execute("INCR", "t")


def test_keys_com_padrao(client):
    for key in ["user:1", "user:2", "post:1"]:
        client.set(key, "x")
    assert client.execute("KEYS", "user:*") == [b"user:1", b"user:2"]
    assert client.execute("KEYS", "*") == [b"post:1", b"user:1", b"user:2"]


def test_compact_e_info(client, store):
    for i in range(20):
        client.set("k", str(i))
    reply = client.execute("COMPACT")
    assert reply.startswith("OK") and "1 keys" in reply
    info = client.execute("INFO").decode()
    assert "minikv_version:" in info
    assert "db0:keys=1" in info
    assert "connected_clients:1" in info
    assert store.get("k") == b"19"


def test_command_e_select_zero_para_o_redis_cli(client):
    assert client.execute("COMMAND", "DOCS") == []
    assert client.execute("SELECT", "0") == "OK"


def test_pipelining_respostas_na_ordem(client):
    replies = client.pipeline(
        ("SET", "a", "1"), ("INCR", "a"), ("GET", "a"), ("GET", "x"), ("NAOEXISTE",)
    )
    assert replies[:4] == ["OK", 2, b"2", None]
    assert isinstance(replies[4], RespError)


def test_comandos_fragmentados_e_inline_em_socket_cru(server):
    with socket.create_connection(("127.0.0.1", server.port), timeout=5) as sock:
        data = encode_command("SET", "frag", "valor\r\nquebrado") + b"GET frag\r\n"
        for i in range(0, len(data), 3):
            sock.sendall(data[i : i + 3])
        parser = RespParser()
        replies = []
        while len(replies) < 2:
            parser.feed(sock.recv(4096))
            replies.extend(parser)
        assert replies == ["OK", b"valor\r\nquebrado"]


def test_erro_de_protocolo_fecha_conexao(server):
    with socket.create_connection(("127.0.0.1", server.port), timeout=5) as sock:
        sock.sendall(b"*1\r\n$-5\r\n")
        data = b""
        while chunk := sock.recv(4096):
            data += chunk
        assert data.startswith(b"-ERR Protocol error")


def test_quit(server):
    with Client("127.0.0.1", server.port) as c:
        assert c.execute("QUIT") == "OK"
        with pytest.raises(ConnectionError):
            c.execute("PING")


def test_varios_clientes_ao_mesmo_tempo(server):
    clients = [Client("127.0.0.1", server.port) for _ in range(5)]
    try:
        for n, c in enumerate(clients):
            c.set(f"cliente:{n}", str(n))
        assert all(c.execute("DBSIZE") == 5 for c in clients)
        for c in clients:
            c.execute("INCR", "compartilhado")
        assert clients[0].get("compartilhado") == b"5"
    finally:
        for c in clients:
            c.close()


def test_hello_resp3_recusado_e_client_aceito(client):
    with pytest.raises(RespError, match="NOPROTO"):
        client.execute("HELLO", "3")
    assert client.execute("HELLO", "2")[:2] == [b"server", b"minikv"]
    assert client.execute("CLIENT", "SETNAME", "teste") == "OK"
