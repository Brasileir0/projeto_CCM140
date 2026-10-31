from datetime import datetime
from pathlib import Path
import msgpack
import zmq

USERS_FILE = Path("users.txt")
CHANNELS_FILE = Path("channels.txt")

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
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if action == "login":
        username = request.get("username")
        if not isinstance(username, str) or not username.strip():
            return {"status": "error", "message": "Nome de usuário inválido"}
        
        username = username.strip()

        with USERS_FILE.open("a", encoding="utf-8") as users:
            users.write(f"{username},{timestamp}\n")

        print(f"[{timestamp}][LOGIN] Usuário: {username}", flush=True)
        return {"status": "success", "message": "Login realizado com sucesso"}

    if action == "create_channel":
        channel = request.get("channel")

        if not isinstance(channel, str) or not channel.strip():
            return {"status": "error", "message": "Nome de canal inválido"}
        
        channel = channel.strip()
        channels = load_channels()

        if channel in channels:
            return {"status": "error", "message": "Canal já existe"}
        
        with CHANNELS_FILE.open("a", encoding="utf-8") as channel_file:
            channel_file.write(f"{channel}\n")

        print(f"[{timestamp}][CHANNEL] Canal criado: {channel}", flush=True)
        return {"status": "success", "message": "Canal criado com sucesso"}

    if action == "list_channels":
        return {"status": "success", "channels": "".join(f"{channel};" for channel in sorted(load_channels()))}

    return {"status": "error", "message": "Ação desconhecida"}

def main() -> None:
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
            socket.send(msgpack.packb(response, use_bin_type=True))
    finally:
        socket.close()
        context.term()

if __name__ == "__main__":
    main()
