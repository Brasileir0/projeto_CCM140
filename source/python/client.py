import os
import time
from datetime import datetime
import msgpack
import zmq


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


def main() -> None:
    username = "PYTHON"
    endpoints = os.environ.get("SERVER_ENDPOINTS", "tcp://server-python:5555").split(",")
    context = zmq.Context()

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
                send_request(socket, {"action": "list_channels"}, "channels")
            finally:
                socket.close()
    finally:
        context.term()


if __name__ == "__main__":
    main()
