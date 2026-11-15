# Protocolo de mensagens

Este documento define o formato das mensagens trocadas entre clientes, servidores, broker, proxy e o serviço de referência. Os clientes e servidores das três linguagens (Python, JavaScript e C++) seguem esta especificação.

## Regras gerais

- **Serialização:** MessagePack, sempre binário (sem JSON, XML ou texto simples na rede).
- **Estrutura:** um mapa. Os valores são textos, com estas exceções:
  - `logical_clock` é um número inteiro, em todas as mensagens;
  - nas respostas da referência, `rank` é um número inteiro e `servers` é uma lista de mapas `{ name, rank }`.
- **Timestamp:** toda mensagem tem o campo `timestamp` com o horário de envio.
  - Os clientes usam `AAAA-MM-DD HH:MM:SS`.
  - Os servidores usam o formato nativo de cada linguagem:
    - Python: ISO 8601 com fuso, por exemplo `2026-11-15T19:07:33.451319+00:00`;
    - JavaScript: ISO 8601 em UTC, por exemplo `2026-11-15T19:07:33.555Z`;
    - C++: `2026-11-15 19:07:33`.
  - A referência usa ISO 8601 em UTC com milissegundos, por exemplo `2026-11-15T19:07:34.856Z`. Esse valor é a **hora correta** usada para sincronizar os servidores.
  - Todos os formatos começam com `AAAA-MM-DD` seguido de `T` ou espaço e de `HH:MM:SS`, e os clientes exibem todos como `AAAA-MM-DD HH:MM:SS`.
- **Relógio lógico:** toda mensagem tem o campo `logical_clock` com o relógio lógico (Lamport) de quem envia. Cada processo começa com 0 e segue duas regras:
  1. **antes de enviar** uma mensagem, soma 1 ao seu relógio e envia o novo valor;
  2. **ao receber** uma mensagem, fica com o maior valor entre o seu relógio e o `logical_clock` recebido.

  Se o `logical_clock` faltar ou não for um número válido, a mensagem é aceita normalmente e o relógio local não muda.

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
| `logical_clock` | sim | relógio lógico do cliente (inteiro) |
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
| `logical_clock` | relógio lógico do servidor (inteiro) |

O texto de `message` pode variar entre as linguagens. O cliente decide o que fazer **apenas pelo `status`**.

### Ações

#### `login`
```
REQ: { action: login, username: CPP-1, timestamp: ..., logical_clock: 1 }
REP: { status: success, message: Login realizado com sucesso, timestamp: ..., logical_clock: 2 }
```
O servidor grava o usuário e o timestamp do login. Se o login falhar, o bot tenta de novo após 1 segundo.

#### `list_channels`
```
REQ: { action: list_channels, timestamp: ..., logical_clock: 3 }
REP: { status: success, channels: canal-a;canal-b, timestamp: ..., logical_clock: 4 }
```

#### `create_channel`
```
REQ: { action: create_channel, channel: canal-CPP-1-1794687526747, timestamp: ..., logical_clock: 5 }
REP: { status: success, message: Canal criado com sucesso, timestamp: ..., logical_clock: 6 }
```
- **Nomes de canal:** qualquer string não vazia (espaços nas pontas são removidos). Os bots usam `canal-<BOT>-<epoch em ms>`, o que evita colisões.
- **Duplicidade:** criar um canal que já existe no servidor retorna `status: error`, `message: Canal já existe`.

#### `publish`
```
REQ: { action: publish, channel: ..., message: ..., username: CPP-1, timestamp: ..., logical_clock: 92 }
PUB: [canal] { channel: ..., message: ..., username: CPP-1, timestamp: ..., logical_clock: 93 }
REP: { status: success, message: Publicação enviada, timestamp: ..., logical_clock: 94 }
```
O servidor grava a publicação em disco, publica no proxy (ver abaixo) e só então responde. A publicação e a resposta são dois envios, então cada uma tem o seu próprio `logical_clock`. Se o canal não existir no servidor que recebeu a requisição (ele pode ter sido criado em outro servidor pelo broker), o canal é registrado nesse servidor antes da publicação.

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

Os textos acima são os do servidor C++. Os servidores Python e JavaScript usam textos equivalentes (por exemplo, `Mensagem inválida ou vazia`). Em todos os casos o `status` é `error`, e a resposta também leva `timestamp` e `logical_clock`.

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
| `logical_clock` | relógio lógico do servidor no momento da publicação (inteiro) |

Cada cliente usa uma única conexão `SUB` e assina vários tópicos nela. Como o filtro do ZeroMQ é por prefixo, o cliente descarta os tópicos que não são exatamente um canal assinado. Ao receber, o cliente atualiza o seu relógio lógico e exibe o canal, o autor, a mensagem, o horário de envio (`timestamp`), o horário local de recebimento, o `logical_clock` da publicação e o seu relógio local depois de recebê-la.

## Serviço de referência (servidor → referência)

| Ligação | Socket | Endereço |
|---|---|---|
| Servidor → referência | `REQ` conecta no `REP` | `tcp://referencia:5559` |

A referência só recebe mensagens dos servidores, nunca dos clientes. Cada servidor se identifica pelo campo `name`, que vem da variável `SERVER_NAME` (por exemplo `server-cpp-1`).

### Requisição

| Campo | Obrigatório | Descrição |
|---|---|---|
| `action` | sim | `rank`, `heartbeat` ou `list` |
| `name` | em `rank` e `heartbeat` | nome do servidor |
| `timestamp` | sim | horário de envio |
| `logical_clock` | sim | relógio lógico do servidor (inteiro) |

### Resposta

| Campo | Descrição |
|---|---|
| `status` | `success` ou `error` |
| `message` | `OK` no heartbeat, ou o motivo do erro |
| `rank` | em `rank` e `heartbeat`: o rank do servidor (inteiro) |
| `servers` | em `list`: lista de `{ name, rank }` dos servidores disponíveis, ordenada pelo rank |
| `timestamp` | hora correta da referência, em ISO 8601 UTC com milissegundos |
| `logical_clock` | relógio lógico da referência (inteiro) |

### Ações

#### `rank`
```
REQ: { action: rank, name: server-cpp-1, timestamp: ..., logical_clock: 1 }
REP: { status: success, rank: 2, timestamp: 2026-11-15T19:07:30.120Z, logical_clock: 3 }
```
Enviado pelo servidor ao iniciar. Um nome novo recebe o próximo rank (1, 2, 3...). Um nome já conhecido recebe sempre o mesmo rank, então não há repetição de nomes. O servidor também passa a constar como disponível.

#### `heartbeat`
```
REQ: { action: heartbeat, name: server-cpp-1, timestamp: ..., logical_clock: 107 }
REP: { status: success, message: OK, rank: 2, timestamp: 2026-11-15T19:07:34.856Z, logical_clock: 110 }
```
Enviado pelo servidor **a cada 10 mensagens de clientes recebidas**. Mantém o servidor na lista de disponíveis. A resposta traz a hora correta no `timestamp`, que o servidor usa para acertar o seu relógio físico.

Se o servidor nunca pediu rank (por exemplo, porque a referência reiniciou), a resposta é `status: error`, `message: Servidor não registrado`, e o servidor pede o `rank` de novo.

#### `list`
```
REQ: { action: list, name: server-cpp-1, timestamp: ..., logical_clock: 111 }
REP: { status: success,
       servers: [ { name: server-javascript-1, rank: 1 }, { name: server-cpp-1, rank: 2 }, ... ],
       timestamp: ..., logical_clock: 112 }
```
Devolve só os servidores disponíveis. O servidor C++ pede a lista depois de cada heartbeat e a mostra no log como `nome:rank;nome:rank`.

### Remoção por falta de heartbeat

A referência guarda o horário do último `rank` ou `heartbeat` de cada servidor. Se um servidor passa mais de **30 segundos** sem se comunicar (configurável com `HEARTBEAT_TIMEOUT_MS`), ele sai da lista devolvida por `list`. Quando volta a mandar `rank` ou `heartbeat`, ele retorna à lista com o mesmo rank de antes.

### Erros

| Situação | `message` |
|---|---|
| Mensagem que não é MessagePack válido | `Requisição inválida` |
| `name` vazio em `rank` ou `heartbeat` | `Nome do servidor inválido` |
| `heartbeat` de um servidor sem rank | `Servidor não registrado` |
| `action` desconhecida | `Ação desconhecida` |

### Sincronização do relógio físico

Cada servidor guarda uma diferença (em ms) entre o seu relógio e o relógio do sistema, e usa o relógio corrigido em todos os timestamps que gera. A correção usa o `timestamp` das respostas da referência:

- **C++ e Python (algoritmo de Cristian):** a hora correta estimada é `timestamp da referência + (hora de recebimento − hora de envio) / 2`, e a diferença é ajustada por `hora estimada − hora de recebimento`. O C++ faz isso a cada heartbeat; o Python, a cada resposta de sucesso da referência (rank e heartbeat).
- **JavaScript:** usa o `timestamp` da referência diretamente, a cada resposta recebida dela.

Se a referência não responder em 2 segundos, o servidor registra o erro, recria a conexão com ela e continua atendendo os clientes.
