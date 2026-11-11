from datetime import datetime
from pathlib import Path
import msgpack
import zmq
import json

DATA_DIR = Path("data")
USERS_FILE = DATA_DIR / "users.txt"
CHANNELS_FILE = DATA_DIR / "channels.txt"
EVENTS_FILE = DATA_DIR / "events.jsonl"

def get_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

        if not isinstance(channel, str) or not channel.strip():
            return{
                "status": "error",
                "message": "Nome de canal inválido"
            }

        if not isinstance(message, str) or not message.strip():
            return{
                    "status": "error",
                    "message": "Mensagem inválida ou vazia"
            }

        channel = channel.strip()
        channels = load_channels()

        if channel not in channels:
            return{
                "status": "error",
                "message": "Canal inválido"
            }

        publication = {
            "channel": channel,
            "message": message,
            "timestamp": get_timestamp()
        }

        persist_event({
            "type": "publication",
            **publication,
            "request_timestamp": timestamp
        })

        publisher.send_multipart([
            channel.encode("utf-8"),
            msgpack.packb(publication, use_bin_type=True)
        ])

        print(
            f"[{publication[timestamp]}][PUB]"
            f"Canal: {channel} | Mensagem: {message}",
            flush=True
            )

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
    socket.bind("tcp://*:5555")

    publisher = context.socket(zmq.PUB)
    publisher.connect("tcp://proxy-pubsub:5557")

    print("[Servidor Python] Iniciado na porta 5555...", flush=True)
    print("[Servidor Python] Publisher conectado na porta 5557", flush=True)

    try:
        while True:
            request = msgpack.unpackb(socket.recv(), raw=False)
            if not isinstance(request, dict):
                response = {"status": "error", "message": "Requisição inválida"}
            else:
                response = handle_request(request, publisher)
            response["timestamp"] = get_timestamp()
            socket.send(msgpack.packb(response, use_bin_type=True))
    finally:
        socket.close()
        publisher.close()
        context.term()

if __name__ == "__main__":
    main()
