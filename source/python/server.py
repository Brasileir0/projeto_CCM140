from datetime import datetime, timedelta, timezone
from pathlib import Path
import os
import time
import msgpack
import zmq
import json

DATA_DIR = Path("data")
USERS_FILE = DATA_DIR / "users.txt"
CHANNELS_FILE = DATA_DIR / "channels.txt"
EVENTS_FILE = DATA_DIR / "events.jsonl"
SERVER_NAME = os.environ.get("SERVER_NAME") or os.environ.get("HOST_NAME") or f"server-python-{os.getpid()}"
REFERENCE_ENDPOINT = os.environ.get("REFERENCE_ENDPOINT", "")
REFERENCE_TIMEOUT_MS = int(os.environ.get("REFERENCE_TIMEOUT_MS", "2000"))
HEARTBEAT_INTERVAL = 10
logical_clock = 0
clock_offset = timedelta(milliseconds=float(os.environ.get("CLOCK_OFFSET_MS", "0")))

def get_now() -> datetime:
    return datetime.now(timezone.utc) + clock_offset

def get_timestamp() -> str:
    return get_now().astimezone().isoformat()

def tick() -> int:
    global logical_clock
    logical_clock += 1
    return logical_clock

def observe_logical_clock(value:object) -> None:
    global logical_clock
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        logical_clock = max(logical_clock, value)

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

def synchronize_clock(reference_timestamp: object, sent_at: datetime, received_at: datetime) -> None:
    global clock_offset
    if not isinstance(reference_timestamp, str):
        return
    try:
        reference_time = datetime.fromisoformat(reference_timestamp.replace("Z", "+00:00"))
    except ValueError:
        print(f"[{get_timestamp()}][RELÓGIO] Hora inválida da referência: {reference_timestamp}", flush=True)
        return
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)

    estimated_now = reference_time + (received_at - sent_at) / 2
    adjusment = estimated_now - received_at
    clock_offset += adjusment
    print(
        f"[{get_timestamp()}][RELÓGIO] Ajuste de {adjusment.total_seconds() * 1000:+.1f} ms "
        f"(diferença atual para o sistema: {clock_offset.total_seconds() * 1000:+.1f} ms)", flush=True
        )

def call_reference(context: zmq.Context, action: str) -> dict | None:
    if not REFERENCE_ENDPOINT:
        return None

    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, REFERENCE_TIMEOUT_MS)
    socket.setsockopt(zmq.SNDTIMEO, REFERENCE_TIMEOUT_MS)

    try:
        socket.connect(REFERENCE_ENDPOINT)
        request = {"action": action, "name": SERVER_NAME}
        request["logical_clock"] = tick()
        request["timestamp"] = get_timestamp()
        print(f"[{request['timestamp']}][REF-REQ] {dict_to_string(request)}", flush=True)

        sent_at = get_now()
        socket.send(msgpack.packb(request, use_bin_type=True))
        raw_response = socket.recv()
        received_at = get_now()

        response = msgpack.unpackb(raw_response, raw=False)
        if not isinstance(response, dict):
            raise ValueError("resposta inválida da referência")
        observe_logical_clock(response.get("logical_clock"))
        print(f"[{get_timestamp()}][REF-REP] {dict_to_string(response)}", flush=True)

        if response.get("status") == "success":
            synchronize_clock(response.get("timestamp"), sent_at, received_at)
        return response
    except zmq.Again:
        print(f"[{get_timestamp()}][REF-ERRO] Referência não respondeu ({action})", flush=True)
        return None

    except Exception as error:
        print(f"[{get_timestamp()}][REF-ERRO] Falha na comunicação com a referência ({action}): {error}", flush=True)
        return None

    finally:
        socket.close()

def request_rank(context: zmq.Context, attempts: int=10) -> int | None:
    for attempt in range(attempts):
        response = call_reference(context, "rank")
        if response and response.get("status") == "success":
            rank = response.get("rank")
            print(f"[{get_timestamp()}][RANK] {SERVER_NAME} tem rank {rank}", flush=True)
            return rank

        if attempt < attempts - 1:
            time.sleep(1)
    print(f"[{get_timestamp()}][RANK] Não foi possível obter rank; seguindo sem referência", flush=True)
    return None

def send_heartbeat(context: zmq.Context) -> None:
    response = call_reference(context, "heartbeat")
    if response is None:
        return
    if response.get("status") != "success":
        print(f"[{get_timestamp()}][HEARTBEAT] Rejeitado: {response.get('message', '')}", flush=True)
        request_rank(context, attempts=1)
        return
    print(f"[{get_timestamp()}][HEARTBEAT] OK", flush=True)

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

        if channel not in channels:
            with CHANNELS_FILE.open("a", encoding="utf-8") as channel_file:
                channel_file.write(f"{channel}\n")
            print(f"[{timestamp}][CHANNEL] Canal registrado a partir de publicação: {channel}", flush=True)

        publication = {
            "channel": channel,
            "message": message,
            "timestamp": get_timestamp()
        }

        publication["logical_clock"] = tick()
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

    if REFERENCE_ENDPOINT:
        request_rank(context)
    client_messages = 0

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
                observe_logical_clock(request.get("logical_clock"))
                print(f"[{received_at}][RECEBIDO] {dict_to_string(request)}", flush=True)
                response = handle_request(request, publisher)
            response["logical_clock"] = tick()
            response["timestamp"] = get_timestamp()
            persist_event({
                "type": "response",
                "timestamp": response["timestamp"],
                "response": response
            })
            print(f"[{response['timestamp']}][ENVIADO] {dict_to_string(response)}", flush=True)
            socket.send(msgpack.packb(response, use_bin_type=True))

            # A cada 10 mensagens de clientes, avisa a referência que está ativo.
            client_messages += 1
            if REFERENCE_ENDPOINT and client_messages % HEARTBEAT_INTERVAL == 0:
                send_heartbeat(context)

    finally:
        socket.close()
        publisher.close()
        context.term()

if __name__ == "__main__":
    main()
