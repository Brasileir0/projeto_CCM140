import os
import random
import threading
import time
from datetime import datetime

import msgpack
import zmq


def get_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M")


def format_timestamp(timestamp: object) -> str:
    if not isinstance(timestamp, str):
        return "desconhecido"
    if len(timestamp) >= 16 and timestamp[10] in ("T", " "):
        return f"{timestamp[:10]} {timestamp[11:16]}"
    return timestamp


def send_request(
    socket: zmq.Socket,
    request: dict[str, str],
    response_field: str = "message",
) -> dict[str, str]:
    timestamp = get_timestamp()
    request["timestamp"] = timestamp
    print(f"[{format_timestamp(timestamp)}][REQ] {request['action']}", flush=True)
    socket.send(msgpack.packb(request, use_bin_type=True))
    response = msgpack.unpackb(socket.recv(), raw=False)
    if not isinstance(response, dict):
        raise ValueError("Resposta inválida do servidor.")
    print(
        f"[{format_timestamp(response.get('timestamp', timestamp))}]"
        f"[REP] {response.get(response_field, response.get('status', ''))}",
        flush=True,
    )
    return response


def receive_messages(endpoint: str, channels: list[str]) -> None:
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(endpoint)
    for channel in channels:
        subscriber.setsockopt_string(zmq.SUBSCRIBE, channel)
    try:
        while True:
            frames = subscriber.recv_multipart()
            if len(frames) != 2:
                print("[ERRO] Formato de publicação inválido", flush=True)
                continue

            channel = frames[0].decode("utf-8")
            publication = msgpack.unpackb(frames[1], raw=False)
            if not isinstance(publication, dict):
                print("[ERRO] Conteúdo de publicação inválido", flush=True)
                continue

            received_at = get_timestamp()
            print(
                "\n---------- NOVA MENSAGEM ---------\n"
                f"Canal: {channel}\n"
                f"Mensagem: {publication.get('message', '')}\n"
                f"Enviada em: {format_timestamp(publication.get('timestamp'))}\n"
                f"Recebida em: {format_timestamp(received_at)}\n"
                "------------------------------------\n",
                flush=True,
            )
    finally:
        subscriber.close()
        context.term()


def list_channels(socket: zmq.Socket) -> list[str]:
    response = send_request(socket, {"action": "list_channels"}, "channels")
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
        print(f"[Cliente Python] Conectado a {endpoint} como {username}", flush=True)
        login = send_request(
            request_socket,
            {"action": "login", "username": username},
        )
        if login.get("status") != "success":
            raise RuntimeError(f"Login falhou: {login.get('message', '')}")

        channels = list_channels(request_socket)
        if len(channels) < 5:
            channel = f"canal-{username}-{int(time.time() * 1000)}"
            created = send_request(
                request_socket,
                {"action": "create_channel", "channel": channel},
            )
            if created.get("status") != "success":
                raise RuntimeError(f"Não foi possível criar o canal {channel}: {created.get('message', '')}")
            channels = list_channels(request_socket)
        if not channels:
            raise RuntimeError("O servidor não retornou canais disponíveis.")

        subscriptions = random.sample(channels, min(3, len(channels)))
        for channel in subscriptions:
            print(f"[SUB] Inscrito no canal: {channel}", flush=True)

        threading.Thread(
            target=receive_messages,
            args=(
                os.environ.get("PUBSUB_XPUB_ENDPOINT", "tcp://proxy-pubsub:5558"),
                subscriptions,
            ),
            daemon=True,
        ).start()
        time.sleep(1)

        while True:
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
                    print(
                        f"[ERRO PUBLICAÇÃO] {response.get('message', '')}",
                        flush=True,
                    )
                time.sleep(1)
    finally:
        request_socket.close()
        context.term()


if __name__ == "__main__":
    main()