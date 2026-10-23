# minikv

Banco chave-valor persistente no estilo Bitcask, com servidor que fala um subconjunto do
protocolo do Redis.

Os dados ficam num log append-only com CRC32 em cada registro, o índice fica num hash em
memória e uma compactação reescreve só o que ainda está vivo. Como o servidor fala RESP2,
bibliotecas cliente do Redis conectam nele. Tudo em Python puro, sem dependências.

[![CI](https://github.com/burnoutscript/minikv/actions/workflows/ci.yml/badge.svg)](https://github.com/burnoutscript/minikv/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Dependências](https://img.shields.io/badge/dependências-zero-success)
![Licença](https://img.shields.io/badge/licença-MIT-lightgrey)

## Por que fiz isso

Na Prumo eu uso PostgreSQL pelo Supabase todos os dias, e confio que um dado que recebeu
"OK" está salvo. Quis entender o que sustenta essa confiança: o que acontece se a energia
cair no meio de uma escrita, para que serve o `fsync` e por que um arquivo que só cresce no
fim é mais fácil de recuperar depois de uma queda.

Comecei pelo paper do Bitcask, o motor de armazenamento do Riak. Ele tem poucas páginas e
descreve um desenho que dá para implementar inteiro: um arquivo onde só se acrescenta, um
índice em memória e uma compactação de tempos em tempos. Implementei esse desenho e escrevi
testes que cortam arquivos no meio para simular a queda. Por último, coloquei na frente um
servidor que fala o protocolo do Redis, para poder testar com clientes que já existem.

## Instalação

```bash
git clone https://github.com/burnoutscript/minikv.git
cd minikv
pip install -e .
```

Python 3.10+. Só a biblioteca padrão.

## Uso

### Linha de comando

O primeiro argumento é o diretório do banco, criado se não existir.

```
$ minikv dados set usuario:1 victor
OK
$ minikv dados set sessao:abc token123 --ttl 3600
OK
$ minikv dados get usuario:1
victor
$ minikv dados incr visitas
1
$ minikv dados keys
sessao:abc
usuario:1
visitas
$ minikv dados ttl sessao:abc
3599.4s
$ minikv dados del usuario:1
1
$ minikv dados get usuario:1
(nil)
$ minikv dados compact
1 arquivo(s), 203 -> 84 bytes, 2 chaves, 42.6 ms
$ minikv dados stats
keys               2
data_files         2
active_file        3
disk_bytes         84
live_bytes         84
recovered_bytes    0
hint_files_loaded  1
sync_policy        always
```

`get` de chave inexistente sai com código 1. Sem subcomando (`minikv dados`), abre um REPL
com os mesmos comandos.

### API Python

```python
from minikv import MiniKV

with MiniKV("dados", sync="interval") as db:
    db.set("usuario:1", "victor")
    db.set("sessao:abc", b"\x00\x01", ttl=3600)
    db.get("usuario:1")          # b"victor"
    db.incr("visitas")           # 1
    db.keys("usuario:*")         # [b"usuario:1"]
    db.delete("usuario:1")       # True
    db.compact()                 # CompactionResult(files_before=1, bytes_before=..., ...)
```

Chaves e valores podem ser `str` (gravado em UTF-8) ou `bytes`. Leituras devolvem `bytes`.
O objeto é thread-safe e pode ser compartilhado entre threads.

### Servidor

```bash
minikv-server --dir dados --port 6380
```

Sessão real com o cliente que vem no pacote, contra o servidor rodando localmente:

```python
>>> c.execute('PING')
'PONG'
>>> c.execute('SET', 'usuario:1', 'victor')
'OK'
>>> c.execute('GET', 'usuario:1')
'victor'
>>> c.execute('SET', 'sessao', 'abc', 'EX', '60')
'OK'
>>> c.execute('TTL', 'sessao')
60
>>> c.execute('INCR', 'visitas')
1
>>> c.execute('KEYS', '*')
['sessao', 'usuario:1', 'visitas']
>>> c.execute('DEL', 'usuario:1')
1
>>> c.execute('DBSIZE')
2
>>> c.execute('COMPACT')
'OK 157 -> 75 bytes, 2 keys'
>>> c.execute('INCR', 'sessao')
'RespError(ERR value is not an integer or out of range)'
```

Testei também com o `redis-py` (usando `protocol=2`), que conecta e roda os mesmos comandos.

Comandos suportados: `PING`, `ECHO`, `GET`, `SET` (com `EX` e `PX`), `DEL`, `EXISTS`, `KEYS`,
`DBSIZE`, `INCR`, `INCRBY`, `DECR`, `EXPIRE`, `TTL`, `PTTL`, `INFO`, `QUIT` e o `COMPACT`,
que não existe no Redis. `COMMAND`, `HELLO 2`, `CLIENT` e `SELECT 0` respondem o mínimo para
que clientes do Redis, como o `redis-py` com `protocol=2`, conectem sem erro.

O pacote traz um cliente pequeno, usado nos testes e no CI:

```python
from minikv import Client

with Client("127.0.0.1", 6380) as c:
    c.set("a", "1", ex=60)
    c.execute("INCR", "visitas")
    c.pipeline(("GET", "a"), ("TTL", "a"))   # [b"1", 60]
```

## Como funciona

### Registro no disco

Cada escrita vira um registro acrescentado ao fim do arquivo ativo. Remoção também é
escrita: um registro com a flag de tombstone.

```
 0        4               12              20     21          25           29
 ┌────────┬───────────────┬───────────────┬──────┬───────────┬────────────┬───────┬───────┐
 │ crc32  │ timestamp_ms  │ expires_at_ms │ flags│ key_size  │ value_size │ chave │ valor │
 │ 4 B    │ 8 B           │ 8 B           │ 1 B  │ 4 B       │ 4 B        │       │       │
 └────────┴───────────────┴───────────────┴──────┴───────────┴────────────┴───────┴───────┘
          └──────────────────────────── coberto pelo CRC32 ───────────────────────────────┘
```

Inteiros em little-endian. `expires_at_ms = 0` quer dizer sem TTL; `flags = 1` é tombstone.

### Índice e arquivos

```
  memória                                      diretório do banco
 ┌───────────────────────────────────┐
 │ índice: dict[bytes, Entry]        │        000004.data + 000004.hint   (compactado)
 │                                   │        ┌────────────────────────────────────┐
 │ b"visitas"   → (4,   36, 1, 0)  ──┼──────▶ │ reg │ reg │ reg │ ...              │
 │ b"usuario:1" → (5, 1043, 6, 0)  ──┼──┐     └────────────────────────────────────┘
 │ b"sessao"    → (5,  958, 3, t)  ──┼──┤     000005.data                 (ativo)
 └───────────────────────────────────┘  │     ┌────────────────────────────────────┐
                                        └───▶ │ reg │ reg │ tombstone │ reg │       │ ◀─ append
   Entry = (arquivo, posição do valor,        └────────────────────────────────────┘
            tamanho do valor, expira_em)
```

Um `get` é uma consulta no dict e uma leitura posicionada no arquivo. Um `set` é um append e
uma atualização no dict. Quando o arquivo ativo passa de `max_file_size` (64 MiB por padrão),
ele vira imutável e um novo começa.

Ao abrir, o minikv lê os arquivos em ordem de id e reconstrói o índice: registro novo
sobrescreve o antigo, tombstone remove. Arquivos que vieram de compactação têm um `.hint` ao
lado, com só chave e posição de cada registro, e são carregados sem ler os valores. O hint tem
CRC por entrada e guarda o tamanho do arquivo de dados que descreve; se algo não bate, o
minikv ignora o hint e varre o arquivo.

### Compactação

Sobrescritas e remoções deixam registros mortos no log. A compactação:

1. Fecha o arquivo ativo e abre outro com id `N+2`, reservando `N+1` para o resultado. Escritas
   novas continuam indo para `N+2` enquanto a compactação roda.
2. Copia para `N+1.data.tmp` só os registros vivos e não expirados do snapshot do índice,
   escreve o hint, faz `fsync` dos dois e troca o nome com `os.replace`.
3. Atualiza o índice só para chaves que não mudaram durante a cópia, e apaga os arquivos antigos
   em ordem crescente.

Como `N+1` fica entre os arquivos antigos e o novo ativo, a ordem dos ids continua sendo a ordem
cronológica, e uma queda em qualquer ponto deixa um estado que a abertura resolve: um `.tmp`
órfão é apagado, e arquivos antigos que sobraram só repetem dados que o arquivo compactado já tem.

### TTL

O instante de expiração vai no registro. Uma chave expirada some na leitura (o `get` devolve
`None` e tira a entrada do índice), na abertura e na compactação, que não copia o registro.
Não existe processo varrendo chaves expiradas em segundo plano.

### Durabilidade (`sync`)

Cada registro é gravado num arquivo aberto sem buffer do Python, então ele chega ao sistema
operacional antes do `set` retornar. A política de `fsync` decide quando o
sistema operacional é obrigado a levar isso para o disco:

| Política | Quando faz fsync | Sobrevive a | Escritas/s (medido abaixo) |
|---|---|---|---|
| `always` | a cada escrita, antes de retornar | queda de energia | ~1.600 |
| `interval` (padrão) | a cada `sync_interval_ms` (1000), numa thread | queda do processo; perde até 1 s numa queda de energia | ~120.000 |
| `never` | só ao rotacionar arquivo; o resto fica a cargo do sistema operacional | queda do processo | ~125.000 |

É o mesmo compromisso do `appendfsync always / everysec / no` do Redis. O CLI usa `always`,
porque cada comando é um processo curto; o servidor usa `interval`.

### Concorrência

- Escritas (`set`, `delete`, `incr`, `expire`) passam por um único lock. Elas só acrescentam
  bytes ao fim do arquivo ativo e atualizam o dict, então seguram o lock por pouco tempo.
- Leituras não pegam esse lock. Uma posição que entrou no índice aponta para bytes que nunca
  mais mudam, e o `get` pode ler em paralelo com um `set`. No Linux a leitura usa `os.pread`; no
  Windows, que não tem `pread`, cada arquivo tem um lock curto em volta do par `seek` + `read`.
- O único momento em que um arquivo deixa de existir é o fim da compactação. Para isso há um
  lock de leitores e escritor: leituras pegam a parte compartilhada, e a troca de arquivos pega
  a exclusiva, esperando as leituras em andamento terminarem antes de fechar e apagar.
- `incr` lê e escreve dentro do lock de escrita, então incrementos concorrentes não se perdem.
- Um arquivo `LOCK` no diretório (com `flock` no Linux e `msvcrt.locking` no Windows) impede que
  dois processos abram o mesmo banco.

O servidor roda os comandos direto no event loop, porque cada um leva microssegundos. A exceção
é o `COMPACT`, que vai para uma thread com `asyncio.to_thread` e não trava os outros clientes.

## Decisões de projeto

### Índice hash em memória em vez de LSM-tree ou B-tree

Com o índice inteiro num dict, uma leitura custa uma busca em memória e no máximo uma leitura
no disco, e uma escrita custa um append. Não há árvore para balancear nem níveis para fundir.
O motor inteiro cabe num arquivo (`store.py`), que dá para ler de uma vez.

O preço é que todas as chaves precisam caber na RAM. Medi cerca de 200 bytes por chave no
CPython com chaves de 10 bytes (o objeto `bytes` da chave, a tupla de posição e a entrada do
dict). Também não existe busca por intervalo: `KEYS` passa por todas as chaves. Uma LSM-tree (LevelDB, RocksDB) resolve as duas coisas mantendo as chaves
ordenadas em disco, mas cada leitura pode consultar vários arquivos e precisa de filtros de Bloom
para não ficar lenta. Para um banco em que o conjunto de chaves é pequeno e os valores podem ser
grandes, o hash em memória é a escolha mais simples que funciona, e foi a que o Bitcask fez.

### Escrita rasgada: como é detectada e por que cortar a cauda é seguro

Se o processo cai ou a energia acaba no meio de um `write`, o fim do arquivo pode ter um
registro pela metade: cabeçalho sem valor, ou bytes que o disco não terminou de gravar. Na
abertura, o minikv lê os registros em sequência e para no primeiro que tem cabeçalho
incompleto, tamanho absurdo, menos bytes do que o cabeçalho promete ou CRC32 diferente do
calculado. O arquivo é truncado nesse ponto e o número de bytes descartados aparece em
`recovered_bytes`.

O CRC é necessário porque o tamanho do arquivo não basta: o sistema de arquivos pode ter
estendido o arquivo com zeros ou lixo antes de os dados chegarem.

Cortar é seguro porque o arquivo só cresce no fim. Tudo antes do registro ruim foi escrito por
completo e passou na verificação, e nada depois dele pode ser válido, já que nenhuma escrita
começa antes da anterior terminar. O registro cortado é a última escrita, e ela não tinha sido
confirmada: com `sync=always`, o `set` só retorna depois do `fsync`, então o cliente nunca
recebeu um `OK` por ela. Os testes simulam isso cortando bytes do fim do arquivo, trocando um
byte do último registro e acrescentando lixo, e conferem que as chaves anteriores continuam lá
e que o banco aceita escritas novas.

## Benchmarks

`python benchmarks/bench.py` com 100 mil chaves e valores de 100 bytes, num Ryzen 7 5700X,
32 GB de RAM, SSD NVMe, Windows 10 e Python 3.12. Os números são indicativos: variam uns 10 a
20% entre execuções e dependem muito do disco (o `fsync` principalmente).

| Medida | Resultado |
|---|---|
| Escrita, `sync=never` (100 mil chaves) | 125.600 ops/s |
| Escrita, `sync=interval` (100 mil chaves) | 119.500 ops/s |
| Escrita, `sync=always` (2 mil chaves) | 1.600 ops/s |
| Leitura aleatória (100 mil chaves) | 126.200 ops/s |
| Abertura varrendo o log (100 mil chaves) | 457 ms |
| Compactação (200 mil registros, 100 mil vivos) | 654 ms (26,3 → 13,2 MiB) |
| Abertura com hint (100 mil chaves) | 130 ms |
| `SET` pela rede, um por vez | 6.100 ops/s |
| `SET` pela rede, pipeline de 100 | 46.200 ops/s |

O hint deixa a abertura 3,5 vezes mais rápida porque não lê os valores nem calcula CRC. A
diferença entre um `SET` por vez e em pipeline é o custo de ida e volta pela rede, mesmo em
localhost.

## Desenvolvimento

```bash
pip install -e ".[dev]"
pytest -v        # 104 testes, uns 4 segundos
ruff check src tests
```

O que os testes cobrem:

- Recuperação de falha: arquivo cortado no meio do último registro, byte trocado, lixo no fim,
  hint corrompido, `.tmp` de uma compactação interrompida.
- Modelo: 1.500 operações aleatórias (set com e sem TTL, delete, get, avanço do relógio,
  compactação, fechar e reabrir) comparadas com um `dict`, com quatro sementes fixas.
- Concorrência: escritores em paralelo, `incr` de 8 threads, leitores durante sobrescritas e
  durante cinco compactações seguidas, escritas durante a compactação.
- TTL com relógio injetado, sem `sleep`.
- Parser RESP alimentado byte a byte, com pipelining e bulk strings que contêm `\r\n`.
- Servidor de verdade em `127.0.0.1` numa porta aleatória, falado por socket.

O CI roda lint e testes em Python 3.10, 3.11 e 3.12 e depois sobe o `minikv-server`, conversa
com ele pelo cliente embutido, derruba o processo e confere com o CLI que o dado ficou no disco.

## Limitações

- Todas as chaves precisam caber na memória. Valores ficam no disco.
- Um nó só. Não há replicação, cluster nem transações (`MULTI`/`EXEC`).
- Só um subconjunto do RESP2, só strings. Listas, hashes, pub/sub, `SCAN` e RESP3 não existem.
  `KEYS` e `DBSIZE` percorrem todas as chaves.
- A compactação é manual (`compact()` ou `COMPACT`). Não há gatilho automático por proporção
  de lixo.
- Corrupção no meio de um arquivo antigo (bit podre, não queda) também trunca a partir dali, e
  os registros seguintes daquele arquivo se perdem. A recuperação foi pensada para a cauda.
- Expiração é preguiçosa: uma chave expirada que ninguém lê ocupa memória até a próxima
  compactação ou abertura.
- É Python. Cerca de 120 mil operações por segundo numa thread serve para estudar e para cargas
  pequenas, e fica longe do Redis, escrito em C.

## Licença

MIT. Veja [LICENSE](LICENSE).
