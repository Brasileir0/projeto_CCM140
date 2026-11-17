import os
import queue
import random
import threading
import time
from datetime import datetime

import msgpack
import zmq

# Canais que a thread principal pede para a thread do SUB assinar.
# Sockets ZeroMQ não são thread-safe, então só a thread do SUB mexe no socket.
pending_subscriptions: "queue.Queue[str]" = queue.Queue()

# As duas threads escrevem na tela; o lock evita que as linhas se misturem.
output_lock = threading.Lock()

logical_clock = 0
clock_lock = threading.Lock()

def tick() -> int:
    global logical_clock
    with clock_lock:
        logical_clock += 1
        return logical_clock

def observe_logical_clock(value: object) -> int:
    global logical_clock
    with clock_lock:
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            logical_clock = max(logical_clock, value)
        return logical_clock

def log(text: str) -> None:
    with output_lock:
        print(text, flush=True)


def get_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Normaliza os timestamps dos servidores
# para "AAAA-MM-DD HH:MM:SS".
def format_timestamp(timestamp: object) -> str:
    if not isinstance(timestamp, str):
        return "desconhecido"
    if len(timestamp) >= 19 and timestamp[10] in ("T", " "):
        return f"{timestamp[:10]} {timestamp[11:19]}"
    return timestamp


def dict_to_string(values: dict) -> str:
    return "{ " + " | ".join(f"{key}: {values[key]}" for key in sorted(values)) + " }"


def send_request(socket: zmq.Socket, request: dict[str, str]) -> dict[str, str]:
    request["logical_clock"] = tick()
    request["timestamp"] = get_timestamp()
    log(f"[{request['timestamp']}][REQ] {dict_to_string(request)}")
    socket.send(msgpack.packb(request, use_bin_type=True))
    response = msgpack.unpackb(socket.recv(), raw=False)
    if not isinstance(response, dict):
        raise ValueError("Resposta inválida do servidor.")
    observe_logical_clock(response.get("logical_clock"))
    log(
        f"[{format_timestamp(response.get('timestamp', get_timestamp()))}]"
        f"[REP] {dict_to_string(response)}"
    )
    return response


def receive_messages(endpoint: str) -> None:
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(endpoint)
    poller = zmq.Poller()
    poller.register(subscriber, zmq.POLLIN)
    subscribed_channels: set[str] = set()
    try:
        while True:
            while not pending_subscriptions.empty():
                channel = pending_subscriptions.get()
                subscriber.setsockopt_string(zmq.SUBSCRIBE, channel)
                subscribed_channels.add(channel)
                log(f"[{get_timestamp()}][SUB] Inscrito no canal: {channel}")

            # Timeout curto para conseguir aplicar novas inscrições da fila.
            if not dict(poller.poll(200)):
                continue

            frames = subscriber.recv_multipart()
            if len(frames) != 2:
                log("[SUB][ERRO] Formato de publicação inválido")
                continue

            channel = frames[0].decode("utf-8")
            # O filtro do ZeroMQ é por prefixo ("canal-a" também casa com "canal-ab").
            if channel not in subscribed_channels:
                continue

            try:
                publication = msgpack.unpackb(frames[1], raw=False)
            except Exception as error:
                log(f"[SUB][ERRO] Publicação inválida: {error}")
                continue
            if not isinstance(publication, dict):
                log("[SUB][ERRO] Conteúdo de publicação inválido")
                continue

            local_clock = observe_logical_clock(publication.get("logical_clock"))
            author = publication.get("username")
            log(
                "\n---------- NOVA MENSAGEM ---------\n"
                f"Canal: {channel}\n"
                + (f"Autor: {author}\n" if author else "")
                + f"Mensagem: {publication.get('message', '')}\n"
                f"Enviada em: {format_timestamp(publication.get('timestamp'))}\n"
                f"Recebida em: {get_timestamp()}\n"
                f"Relógio lógico: {publication.get('logical_clock', 'indisponível')} (local após receber: {local_clock})\n"
                "------------------------------------\n"
            )
    finally:
        subscriber.close()
        context.term()


def list_channels(socket: zmq.Socket) -> list[str]:
    response = send_request(socket, {"action": "list_channels"})
    if response.get("status") != "success":
        raise RuntimeError(f"Não foi possível listar canais: {response.get('message', '')}")

    channels = response.get("channels", "")
    if isinstance(channels, list):
        return [channel for channel in channels if isinstance(channel, str) and channel]
    if isinstance(channels, str):
        return [channel.strip() for channel in channels.split(";") if channel.strip()]
    raise ValueError("A resposta do servidor contém uma lista de canais inválida.")


def main() -> None:
    username = os.environ.get("BOT_NAME", f"PYTHON-{os.getpid()}")
    endpoints = [
        endpoint.strip()
        for endpoint in os.environ.get("SERVER_ENDPOINTS", "tcp://server-python:5555").split(",")
        if endpoint.strip()
    ]
    if not endpoints:
        raise ValueError("SERVER_ENDPOINTS não contém endpoints válidos.")

    context = zmq.Context()
    request_socket = context.socket(zmq.REQ)
    endpoint = random.choice(endpoints)
    request_socket.connect(endpoint)

    try:
        log(f"[Cliente Python] Conectado a {endpoint} como {username}")

        # Em caso de erro, o login é repetido (requisito da parte 1).
        while True:
            login = send_request(
                request_socket,
                {"action": "login", "username": username},
            )
            if login.get("status") == "success":
                break
            log(f"[Cliente Python] Login falhou: {login.get('message', '')}. Tentando novamente...")
            time.sleep(1)

        threading.Thread(
            target=receive_messages,
            args=(os.environ.get("PUBSUB_XPUB_ENDPOINT", "tcp://proxy-pubsub:5558"),),
            daemon=True,
        ).start()

        subscriptions: set[str] = set()
        while True:
            # 1. Se existirem menos do que 5 canais, cria um novo.
            channels = list_channels(request_socket)
            if len(channels) < 5:
                channel = f"canal-{username}-{int(time.time() * 1000)}"
                created = send_request(
                    request_socket,
                    {"action": "create_channel", "channel": channel},
                )
                if created.get("status") == "success":
                    channels = list_channels(request_socket)
                else:
                    log(f"[Cliente Python] Não foi possível criar o canal {channel}: {created.get('message', '')}")
            if not channels:
                log("[Cliente Python] Nenhum canal disponível, tentando novamente...")
                time.sleep(1)
                continue

            # 2. Se estiver inscrito em menos do que 3 canais, inscreve-se em mais um.
            if len(subscriptions) < 3:
                candidates = [channel for channel in channels if channel not in subscriptions]
                if candidates:
                    channel = random.choice(candidates)
                    subscriptions.add(channel)
                    pending_subscriptions.put(channel)

            # 3. Escolhe um canal e envia 10 mensagens com 1 segundo de intervalo.
            channel = random.choice(channels)
            for index in range(10):
                message = (
                    f"Mensagem {index + 1} de {username}: "
                    f"{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))}"
                )
                response = send_request(
                    request_socket,
                    {
                        "action": "publish",
                        "channel": channel,
                        "message": message,
                        "username": username,
                    },
                )
                if response.get("status") != "success":
                    log(f"[ERRO PUBLICAÇÃO] {response.get('message', '')}")
                time.sleep(1)
    finally:
        request_socket.close()
        context.term()


if __name__ == "__main__":
    main()