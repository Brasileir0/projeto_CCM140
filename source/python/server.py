from datetime import datetime
from pathlib import Path
import msgpack
import zmq

DATA_DIR = Path("data")
USERS_FILE = DATA_DIR / "users.txt"
CHANNELS_FILE = DATA_DIR / "channels.txt"

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

def handle_request(request: dict[str, object]) -> dict[str, str]:
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

    print(f"[{timestamp}][ERRO] Ação desconhecida: {action}", flush=True)
    return {"status": "error", "message": "Ação desconhecida"}

def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5555")
    print("[Servidor Python] Iniciado na porta 5555...", flush=True)

    try:
        while True:
            request = msgpack.unpackb(socket.recv(), raw=False)
            if not isinstance(request, dict):
                response = {"status": "error", "message": "Requisição inválida"}
            else:
                response = handle_request(request)
            response["timestamp"] = get_timestamp()
            socket.send(msgpack.packb(response, use_bin_type=True))
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
