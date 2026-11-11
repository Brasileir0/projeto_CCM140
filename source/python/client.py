import os
import time
from datetime import datetime
import msgpack
import zmq
import random


def send_request(
    socket: zmq.Socket,
    request: dict[str, str],
    response_field: str,
) -> dict[str, str]:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    request["timestamp"] = timestamp
    print(f"[{timestamp}][REQ] {request['action']}", flush=True)
    socket.send(msgpack.packb(request, use_bin_type=True))
    response = msgpack.unpackb(socket.recv(), raw=False)
    if not isinstance(response, dict):
        raise ValueError("Resposta inválida do servidor.")
    print(f"[{timestamp}][REP] {response.get(response_field, '')}", flush=True)
    return response

def receive_message(subscriber: zmq.Socket) -> None:
    frames = subscriber.recv_multipart()

    if len(frames) != 2:
        print("[ERRO] Formato de publicação inválido", flush=True)
        return

    channel = frames[0].decode("utf-8")

    publication = msgpack.unpackb(frames[1], raw=False)

    received_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    message = publication.get("message", "")
    sent_at = publication.get("timestamp", "")

    print("\n========== NOVA MENSAGEM ==========", flush=True)
    print(f"Canal: {channel}", flush=True)
    print(f"Mensagem: {message}", flush=True)
    print(f"Enviada em: {sent_at}", flush=True)
    print(f"Recebida em: {received_at}", flush=True)
    print("===================================\n", flush=True)

def publish_message(socket: zmq.Socket, channel: str, message: str) -> None:
    request = {
        "action": "publish",
        "channel": channel,
        "message": message
    }   

    response = send_request(socket, request, "message")

    if response.get("status") == "success":
        print(f"[PUBLICAÇÃO] Mensagem enviada ao canal {channel}", flush=True)

    else:
        print(f"[ERRO PUBLICAÇÃO] {response.get('message', '')}", flush=True)

def main() -> None:
    username = "PYTHON"
    endpoints = os.environ.get("SERVER_ENDPOINTS", "tcp://server-python:5555").split(",")
    context = zmq.Context()

    subscriber = context.socket(zmq.SUB)
    subscriber.connect("tcp://proxy-pubsub:5558")


    try:
        for endpoint in endpoints:
            endpoint = endpoint.strip()
            if not endpoint:
                continue
            print(f"[Cliente Python] Conectando a {endpoint}", flush=True)
            socket = context.socket(zmq.REQ)
            socket.connect(endpoint)
            try:
                login_request = {"action": "login", "username": username}
                login = send_request(socket, login_request, "message")
                while login.get("status") != "success":
                    time.sleep(1)
                    login = send_request(socket, login_request.copy(), "message")

                send_request(
                    socket,
                    {"action": "create_channel", "channel": "geral"},
                    "message",
                )

                channel_response = send_request(
                    socket,
                    {"action": "list_channels"},
                    "channels"
                )

                channels = channel_response.get("channels", "").split(";")
                channels = [channel for channel in channels if channel]

                select_channels = random.sample(
                    channels,
                    min(3, len(channels))
                )

                for channel in select_channels:
                    subscriber.setsockopt_string(zmq.SUBSCRIBE, channel)
                    print(f"[SUB] Inscrito no canal: {channel}", flush=True)

            finally:
                socket.close()

            poller = zmq.Poller()
            poller.register(subscriber, zmq.POLLIN)
        
            while True:
                events = dict(poller.poll(timeout=1000))
        
                if subscriber in events:
                    if events[subscriber] == zmq.POLLIN:
                        receive_message(subscriber)
            
    finally:
        subscriber.close()
        context.term()


if __name__ == "__main__":
    main()