import pytest

from minikv.resp import (
    NEED_MORE,
    ProtocolError,
    RespError,
    RespParser,
    encode_array,
    encode_bulk,
    encode_command,
    encode_error,
    encode_integer,
    encode_simple,
)


def parse_one(data: bytes):
    parser = RespParser()
    parser.feed(data)
    return parser.get()


@pytest.mark.parametrize(
    "data, expected",
    [
        (b"+OK\r\n", "OK"),
        (b":42\r\n", 42),
        (b":-7\r\n", -7),
        (b"$5\r\nhello\r\n", b"hello"),
        (b"$0\r\n\r\n", b""),
        (b"$-1\r\n", None),
        (b"*-1\r\n", None),
        (b"*0\r\n", []),
        (b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", [b"GET", b"k"]),
        (b"*2\r\n:1\r\n*1\r\n+x\r\n", [1, ["x"]]),
    ],
)
def test_tipos_basicos(data, expected):
    assert parse_one(data) == expected


def test_erro_vira_resp_error():
    value = parse_one(b"-ERR deu ruim\r\n")
    assert isinstance(value, RespError)
    assert str(value) == "ERR deu ruim"


def test_bulk_com_crlf_dentro():
    payload = b"linha1\r\nlinha2\r\n"
    assert parse_one(encode_bulk(payload)) == payload


def test_leitura_parcial_byte_a_byte():
    data = encode_command("SET", "chave", "valor\r\ncom quebra")
    parser = RespParser()
    for byte in data[:-1]:
        parser.feed(bytes([byte]))
        assert parser.get() is NEED_MORE
    parser.feed(data[-1:])
    assert parser.get() == [b"SET", b"chave", b"valor\r\ncom quebra"]
    assert parser.get() is NEED_MORE


def test_pipelining_varios_comandos_num_bloco():
    parser = RespParser()
    parser.feed(encode_command("PING") + encode_command("GET", "a") + b"*1\r\n$4\r\nPI")
    assert list(parser) == [[b"PING"], [b"GET", b"a"]]
    parser.feed(b"NG\r\n")
    assert parser.get() == [b"PING"]


def test_comando_inline():
    assert parse_one(b"SET a 1\r\n") == [b"SET", b"a", b"1"]
    assert parse_one(b"\r\n") == []


@pytest.mark.parametrize(
    "data",
    [b"$-2\r\n", b"$abc\r\n", b":1.5\r\n", b"*-3\r\n", b"$3\r\nabcXY", b"$ 3\r\nabc\r\n"],
)
def test_entrada_invalida_e_erro_de_protocolo(data):
    with pytest.raises(ProtocolError):
        parse_one(data)


def test_linha_sem_fim_longa_demais():
    with pytest.raises(ProtocolError):
        parse_one(b"+" + b"x" * 70_000)


def test_arrays_aninhados_demais():
    with pytest.raises(ProtocolError):
        parse_one(b"*1\r\n" * 40 + b":1\r\n")


def test_codificacao():
    assert encode_simple("OK") == b"+OK\r\n"
    assert encode_error("ERR a\r\nb") == b"-ERR a  b\r\n"
    assert encode_integer(-3) == b":-3\r\n"
    assert encode_bulk(None) == b"$-1\r\n"
    assert encode_array([b"a", b""]) == b"*2\r\n$1\r\na\r\n$0\r\n\r\n"
    assert encode_command("SET", b"k", 10) == b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n10\r\n"
