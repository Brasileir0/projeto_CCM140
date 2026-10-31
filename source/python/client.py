from datetime import datetime
import msgpack
import zmq


def send_request(socket: zmq.Socket, request: dict[str, str]) -> dict[str, str]:
    socket.send(msgpack.packb(request, use_bin_type=True))
    response = msgpack.unpackb(socket.recv(), raw=False)
    if not isinstance(response, dict):
        raise ValueError("Resposta inválida do servidor.")
    return response


def main() -> None:
    username = "PYTHON"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://server-python:5555")

    try:
        login = send_request(
            socket,
            {"action": "login", "username": username, "timestamp": timestamp},
        )
        print(f"[{timestamp}][LOGIN RESP] {login.get('message', '')}")

        channel = send_request(
            socket,
            {"action": "create_channel", "channel": "geral", "timestamp": timestamp},
        )
        print(f"[{timestamp}][CHANNEL RESP] {channel.get('message', '')}")

        channels = send_request(socket, {"action": "list_channels", "timestamp": timestamp})
        print(f"[{timestamp}][CANAIS] {channels.get('channels', '')}")
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    main()
