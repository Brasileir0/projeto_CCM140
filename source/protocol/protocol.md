# Protocolo de mensagens

Este documento define o formato das mensagens trocadas entre clientes, servidores, broker e proxy. Os clientes e servidores das três linguagens (Python, JavaScript e C++) seguem esta especificação.

## Regras gerais

- **Serialização:** MessagePack, sempre binário (sem JSON, XML ou texto simples na rede).
- **Estrutura:** um mapa `string → string`. Todos os valores são strings, inclusive listas (ver `list_channels`).
- **Timestamp:** toda mensagem tem o campo `timestamp` com o horário de envio.
  - Os clientes usam `AAAA-MM-DD HH:MM:SS`.
  - Os servidores usam o formato nativo de cada linguagem:
    - Python: ISO 8601 com fuso, por exemplo `2026-11-14T20:18:46.123456+00:00`;
    - JavaScript: ISO 8601 em UTC, por exemplo `2026-11-14T20:18:46.742Z`;
    - C++: `2026-11-14 20:18:46`.
  - Todos os formatos começam com `AAAA-MM-DD` seguido de `T` ou espaço e de `HH:MM:SS`, e os clientes exibem todos como `AAAA-MM-DD HH:MM:SS`.

## Req-Rep (cliente → broker → servidor)

| Ligação | Socket | Endereço |
|---|---|---|
| Cliente → broker | `REQ` conecta no `ROUTER` | `tcp://broker:5555` |
| Servidor → broker | `REP` conecta no `DEALER` | `tcp://broker:5556` |

Cada requisição tem um frame com o mapa MessagePack. A resposta também.

### Requisição

| Campo | Obrigatório | Descrição |
|---|---|---|
| `action` | sim | `login`, `list_channels`, `create_channel` ou `publish` |
| `timestamp` | sim | horário de envio |
| `username` | em `login` e `publish` | nome do bot |
| `channel` | em `create_channel` e `publish` | nome do canal |
| `message` | em `publish` | texto da publicação |

### Resposta

| Campo | Descrição |
|---|---|
| `status` | `success` ou `error` |
| `message` | texto informativo ou motivo do erro (ausente em `list_channels`) |
| `channels` | só em `list_channels`: nomes dos canais separados por `;` (string vazia se não houver canais) |
| `timestamp` | horário de envio da resposta |

O texto de `message` pode variar entre as linguagens. O cliente decide o que fazer **apenas pelo `status`**.

### Ações

#### `login`
```
REQ: { action: login, username: CPP-1, timestamp: ... }
REP: { status: success, message: Login realizado com sucesso, timestamp: ... }
```
O servidor grava o usuário e o timestamp do login. Se o login falhar, o bot tenta de novo após 1 segundo.

#### `list_channels`
```
REQ: { action: list_channels, timestamp: ... }
REP: { status: success, channels: canal-a;canal-b, timestamp: ... }
```

#### `create_channel`
```
REQ: { action: create_channel, channel: canal-CPP-1-1794687526747, timestamp: ... }
REP: { status: success, message: Canal criado com sucesso, timestamp: ... }
```
- **Nomes de canal:** qualquer string não vazia (espaços nas pontas são removidos). Os bots usam `canal-<BOT>-<epoch em ms>`, o que evita colisões.
- **Duplicidade:** criar um canal que já existe no servidor retorna `status: error`, `message: Canal já existe`.

#### `publish`
```
REQ: { action: publish, channel: ..., message: ..., username: CPP-1, timestamp: ... }
REP: { status: success, message: Publicação enviada, timestamp: ... }
```
O servidor grava a publicação em disco, publica no proxy (ver abaixo) e só então responde. Se o canal não existir no servidor que recebeu a requisição (ele pode ter sido criado em outro servidor pelo broker), o canal é registrado nesse servidor antes da publicação.

### Erros

| Situação | `message` |
|---|---|
| Mensagem que não é MessagePack válido | `Requisição inválida` |
| `timestamp` ausente ou vazio | `Timestamp ausente` |
| `username` vazio no `login` | `Nome de usuário inválido` |
| `channel` vazio | `Nome de canal inválido` |
| Canal já existe no `create_channel` | `Canal já existe` |
| `channel` ou `message` vazios no `publish` | `Canal ou mensagem inválidos` |
| `action` desconhecida | `Ação desconhecida` |

Os textos acima são os do servidor C++. Os servidores Python e JavaScript usam textos equivalentes (por exemplo, `Mensagem inválida ou vazia`). Em todos os casos o `status` é `error`.

## Pub/Sub (servidor → proxy → cliente)

| Ligação | Socket | Endereço |
|---|---|---|
| Servidor → proxy | `PUB` conecta no `XSUB` | `tcp://proxy-pubsub:5557` |
| Cliente → proxy | `SUB` conecta no `XPUB` | `tcp://proxy-pubsub:5558` |

Cada publicação é uma mensagem com **2 frames**:

1. **tópico:** o nome do canal, em bytes UTF-8;
2. **conteúdo:** um mapa MessagePack:

| Campo | Descrição |
|---|---|
| `channel` | nome do canal |
| `message` | texto publicado |
| `username` | autor da publicação |
| `timestamp` | horário em que o servidor publicou |

Cada cliente usa uma única conexão `SUB` e assina vários tópicos nela. Como o filtro do ZeroMQ é por prefixo, o cliente descarta os tópicos que não são exatamente um canal assinado. Ao receber, o cliente exibe o canal, o autor, a mensagem, o horário de envio (`timestamp`) e o horário local de recebimento.
