from datetime import datetime
from pathlib import Path
import os
import msgpack
import zmq
import json

DATA_DIR = Path("data")
USERS_FILE = DATA_DIR / "users.txt"
CHANNELS_FILE = DATA_DIR / "channels.txt"
EVENTS_FILE = DATA_DIR / "events.jsonl"

def get_timestamp() -> str:
    return datetime.now().astimezone().isoformat()

def dict_to_string(values: dict) -> str:
    return "{ " + " | ".join(f"{key}: {values[key]}" for key in sorted(values)) + " }"

def load_channels() -> set[str]:
    if not CHANNELS_FILE.exists():
        return set()
    return {
        channel.strip()
        for channel in CHANNELS_FILE.read_text(encoding="utf-8").splitlines()
        if channel.strip()
    }

def handle_request(request: dict[str, object], publisher: zmq.Socket) -> dict[str, object]:
    action = request.get("action")
    timestamp = request.get("timestamp")
    if not isinstance(timestamp, str) or not timestamp.strip():
        print(f"[{get_timestamp()}][ERRO] Timestamp ausente", flush=True)
        return {"status": "error", "message": "Timestamp ausente"}

    if action == "login":
        username = request.get("username")
        if not isinstance(username, str) or not username.strip():
            print(f"[{get_timestamp()}][LOGIN ERRO] Nome inválido", flush=True)
            return {"status": "error", "message": "Nome de usuário inválido"}

        username = username.strip()

        with USERS_FILE.open("a", encoding="utf-8") as users:
            users.write(f"{username},{timestamp}\n")

        print(f"[{timestamp}][LOGIN] Usuário: {username}", flush=True)
        return {"status": "success", "message": "Login realizado com sucesso"}

    if action == "create_channel":
        channel = request.get("channel")

        if not isinstance(channel, str) or not channel.strip():
            print(f"[{get_timestamp()}][CANAL ERRO] Nome inválido", flush=True)
            return {"status": "error", "message": "Nome de canal inválido"}

        channel = channel.strip()
        channels = load_channels()

        if channel in channels:
            print(f"[{timestamp}][CANAL ERRO] Canal já existe: {channel}", flush=True)
            return {"status": "error", "message": "Canal já existe"}

        with CHANNELS_FILE.open("a", encoding="utf-8") as channel_file:
            channel_file.write(f"{channel}\n")

        print(f"[{timestamp}][CHANNEL] Canal criado: {channel}", flush=True)
        return {"status": "success", "message": "Canal criado com sucesso"}

    if action == "list_channels":
        channels = ";".join(sorted(load_channels()))
        print(f"[{timestamp}][CANAIS] Lista enviada: {channels}", flush=True)
        return {"status": "success", "channels": channels}

    if action == "publish":
        channel = request.get("channel")
        message = request.get("message")
        username = request.get("username")

        if not isinstance(channel, str) or not channel.strip():
            print(f"[{timestamp}][PUB ERRO] Nome de canal inválido", flush=True)
            return{
                "status": "error",
                "message": "Nome de canal inválido"
            }

        if not isinstance(message, str) or not message.strip():
            print(f"[{timestamp}][PUB ERRO] Mensagem inválida ou vazia", flush=True)
            return{
                    "status": "error",
                    "message": "Mensagem inválida ou vazia"
            }

        channel = channel.strip()
        channels = load_channels()

        # Com o broker, o canal pode ter sido criado em outro servidor;
        # ele passa a ser registrado também neste.
        if channel not in channels:
            with CHANNELS_FILE.open("a", encoding="utf-8") as channel_file:
                channel_file.write(f"{channel}\n")
            print(f"[{timestamp}][CHANNEL] Canal registrado a partir de publicação: {channel}", flush=True)

        publication = {
            "channel": channel,
            "message": message,
            "timestamp": get_timestamp()
        }
        if isinstance(username, str) and username.strip():
            publication["username"] = username.strip()

        persist_event({
            "type": "publication",
            **publication,
            "request_timestamp": timestamp
        })

        publisher.send_multipart([
            channel.encode("utf-8"),
            msgpack.packb(publication, use_bin_type=True)
        ])

        print(f"[{publication['timestamp']}][PUB] {dict_to_string(publication)}", flush=True)

        return{
            "status": "success",
            "message": "Mensagem publicada"
        }


    print(f"[{timestamp}][ERRO] Ação desconhecida: {action}", flush=True)
    return {"status": "error", "message": "Ação desconhecida"}

def persist_event(event: dict[str, object]) -> None:
    with EVENTS_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, ensure_ascii=False) + "\n")

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    # Com BROKER_BACKEND definido, o servidor conecta no DEALER do broker;
    # sem ele, atende os clientes diretamente na porta 5555.
    broker_backend = os.environ.get("BROKER_BACKEND")
    if broker_backend:
        socket.connect(broker_backend)
        print(f"[Servidor Python] Conectado ao broker em {broker_backend}", flush=True)
    else:
        socket.bind("tcp://*:5555")
        print("[Servidor Python] Iniciado na porta 5555...", flush=True)

    publisher = context.socket(zmq.PUB)
    publisher.connect(os.environ.get("PUBSUB_XSUB_ENDPOINT", "tcp://proxy-pubsub:5557"))
    print("[Servidor Python] Publisher conectado ao proxy Pub/Sub", flush=True)

    try:
        while True:
            raw_request = socket.recv()
            received_at = get_timestamp()
            try:
                request = msgpack.unpackb(raw_request, raw=False)
            except Exception as error:
                print(f"[{received_at}][ERRO] Requisição inválida: {error}", flush=True)
                request = None
            persist_event({
                "type": "request",
                "received_at": received_at,
                "request": request
            })
            if not isinstance(request, dict):
                response = {"status": "error", "message": "Requisição inválida"}
            else:
                print(f"[{received_at}][RECEBIDO] {dict_to_string(request)}", flush=True)
                response = handle_request(request, publisher)
            response["timestamp"] = get_timestamp()
            persist_event({
                "type": "response",
                "timestamp": response["timestamp"],
                "response": response
            })
            print(f"[{response['timestamp']}][ENVIADO] {dict_to_string(response)}", flush=True)
            socket.send(msgpack.packb(response, use_bin_type=True))
    finally:
        socket.close()
        publisher.close()
        context.term()

if __name__ == "__main__":
    main()
