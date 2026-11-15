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

SERVER_NAME = os.environ.get("SERVER_NAME") or os.environ.get("HOSTNAME") or f"server-python-{os.getpid()}"
REFERENCE_ENDPOINT = os.environ.get("REFERENCE_ENDPOINT", "")
REFERENCE_TIMEOUT_MS = int(os.environ.get("REFERENCE_TIMEOUT_MS", "2000"))

PEER_BIND_ENDPOINT = os.environ.get("PEER_BIND_ENDPOINT", "tcp://*:5560")
PEER_PORT = os.environ.get("PEER_PORT", "5560")
PEER_TIMEOUT_MS = int(os.environ.get("PEER_TIMEOUT_MS", "1000"))

PEER_ENDPOINTS = dict(
    item.split("=", 1) for item in os.environ.get("PEER_ENDPOINTS", "").split(",") if "=" in item
)

SERVERS_TOPIC = "servers"
PUBSUB_XPUB_ENDPOINT = os.environ.get("PUBSUB_XPUB_ENDPOINT", "tcp://proxy-pubsub:5558")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "10"))
CLOCK_SYNC_INTERVAL = int(os.environ.get("CLOCK_SYNC_INTERVAL", "15"))
logical_clock = 0
clock_offset = timedelta(milliseconds=float(os.environ.get("CLOCK_OFFSET_MS", "0")))
my_rank: int | None = None
coordinator: str | None = None


def get_now() -> datetime:
    return datetime.now(timezone.utc) + clock_offset


def get_timestamp() -> str:
    return get_now().astimezone().isoformat()

def tick() -> int:
    global logical_clock
    logical_clock += 1
    return logical_clock

def observe_logical_clock(value: object) -> None:
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
        print(f"[{get_timestamp()}][RELÓGIO] Hora inválida do coordenador: {reference_timestamp}", flush=True)
        return
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)

    estimated_now = reference_time + (received_at - sent_at) / 2
    adjustment = estimated_now - received_at
    clock_offset += adjustment
    print(
        f"[{get_timestamp()}][RELÓGIO] Ajuste de {adjustment.total_seconds() * 1000:+.1f} ms "
        f"com a hora de {coordinator} (diferença atual para o sistema: "
        f"{clock_offset.total_seconds() * 1000:+.1f} ms)",
        flush=True,
    )

def send_request(
    context: zmq.Context, endpoint: str, request: dict, tag: str, timeout_ms: int
) -> tuple[dict, datetime, datetime] | None:
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
    try:
        socket.connect(endpoint)
        request["logical_clock"] = tick()
        request["timestamp"] = get_timestamp()
        print(f"[{request['timestamp']}][{tag}-REQ] {dict_to_string(request)}", flush=True)

        sent_at = get_now()
        socket.send(msgpack.packb(request, use_bin_type=True))
        raw_response = socket.recv()
        received_at = get_now()

        response = msgpack.unpackb(raw_response, raw=False)
        if not isinstance(response, dict):
            raise ValueError("resposta inválida")
        observe_logical_clock(response.get("logical_clock"))
        print(f"[{get_timestamp()}][{tag}-REP] {dict_to_string(response)}", flush=True)
        return response, sent_at, received_at
    except zmq.Again:
        print(f"[{get_timestamp()}][{tag}-ERRO] Sem resposta de {endpoint} ({request.get('action')})", flush=True)
        return None
    except Exception as error:
        print(f"[{get_timestamp()}][{tag}-ERRO] Falha ao falar com {endpoint} ({request.get('action')}): {error}", flush=True)
        return None
    finally:
        socket.close()

def call_reference(context: zmq.Context, action: str) -> dict | None:
    if not REFERENCE_ENDPOINT:
        return None
    result = send_request(
        context, REFERENCE_ENDPOINT, {"action": action, "name": SERVER_NAME}, "REF", REFERENCE_TIMEOUT_MS
    )
    return result[0] if result else None

def request_rank(context: zmq.Context, attempts: int = 10) -> int | None:
    global my_rank
    for attempt in range(attempts):
        response = call_reference(context, "rank")
        if response and response.get("status") == "success":
            try:
                my_rank = int(response.get("rank"))
            except (TypeError, ValueError):
                my_rank = None
            if my_rank is not None:
                print(f"[{get_timestamp()}][RANK] {SERVER_NAME} tem rank {my_rank}", flush=True)
                return my_rank
        if attempt < attempts - 1:
            time.sleep(1)
    print(f"[{get_timestamp()}][RANK] Não foi possível obter o rank; seguindo sem referência", flush=True)
    return None

def fetch_servers(context: zmq.Context) -> list[tuple[str, int]]:
    response = call_reference(context, "list")
    if not response or response.get("status") != "success":
        return []
    servers = []
    if isinstance(response.get("servers"), list):
        for item in response["servers"]:
            try:
                servers.append((str(item["name"]), int(item["rank"])))
            except (KeyError, TypeError, ValueError):
                continue
    return servers

def send_heartbeat(context: zmq.Context) -> None:
    response = call_reference(context, "heartbeat")
    if response is None:
        return
    if response.get("status") != "success":
        print(f"[{get_timestamp()}][HEARTBEAT] Rejeitado: {response.get('message', '')}", flush=True)
        request_rank(context, attempts=1)
        return
    print(f"[{get_timestamp()}][HEARTBEAT] OK", flush=True)

def peer_endpoint(name: str) -> str:
    return PEER_ENDPOINTS.get(name) or f"tcp://{name}:{PEER_PORT}"


def call_peer(context: zmq.Context, name: str, action: str) -> tuple[dict, datetime, datetime] | None:
    request = {"action": action, "name": SERVER_NAME, "rank": str(my_rank)}
    return send_request(context, peer_endpoint(name), request, "PEER", PEER_TIMEOUT_MS)

def announce_coordinator(publisher: zmq.Socket) -> None:
    announcement = {
        "coordinator": SERVER_NAME,
        "rank": str(my_rank),
        "logical_clock": tick(),
        "timestamp": get_timestamp(),
    }
    publisher.send_multipart([
        SERVERS_TOPIC.encode("utf-8"),
        msgpack.packb(announcement, use_bin_type=True),
    ])
    print(f"[{announcement['timestamp']}][PUB] [{SERVERS_TOPIC}] {dict_to_string(announcement)}", flush=True)

def run_election(context: zmq.Context, publisher: zmq.Socket) -> None:
    global coordinator
    if my_rank is None and request_rank(context, attempts=1) is None:
        print(f"[{get_timestamp()}][ELEIÇÃO] Sem rank, não é possível eleger", flush=True)
        return

    print(f"[{get_timestamp()}][ELEIÇÃO] {SERVER_NAME} (rank {my_rank}) iniciou uma eleição", flush=True)
    coordinator = None
    higher = sorted(
        ((name, rank) for name, rank in fetch_servers(context) if rank > my_rank and name != SERVER_NAME),
        key=lambda server: server[1],
    )
    for name, rank in higher:
        result = call_peer(context, name, "election")
        if result and result[0].get("status") == "success":
            print(f"[{get_timestamp()}][ELEIÇÃO] {name} (rank {rank}) respondeu OK e assume a eleição", flush=True)
            return

    coordinator = SERVER_NAME
    print(f"[{get_timestamp()}][COORDENADOR] {SERVER_NAME} (rank {my_rank}) é o novo coordenador", flush=True)
    announce_coordinator(publisher)

def synchronize_with_coordinator(context: zmq.Context, publisher: zmq.Socket) -> None:
    global coordinator
    if not REFERENCE_ENDPOINT:
        return
    if coordinator is None:
        run_election(context, publisher)
        return
    if coordinator == SERVER_NAME:
        print(f"[{get_timestamp()}][RELÓGIO] {SERVER_NAME} é o coordenador: o relógio dele é a referência", flush=True)
        return

    result = call_peer(context, coordinator, "clock")
    if result is None or result[0].get("status") != "success":
        print(f"[{get_timestamp()}][COORDENADOR] {coordinator} indisponível, pedindo eleição", flush=True)
        coordinator = None
        run_election(context, publisher)
        return
    response, sent_at, received_at = result
    synchronize_clock(response.get("timestamp"), sent_at, received_at)

def handle_announcement(frames: list[bytes]) -> bool:
    global coordinator
    if len(frames) != 2 or frames[0].decode("utf-8", errors="replace") != SERVERS_TOPIC:
        return False
    try:
        announcement = msgpack.unpackb(frames[1], raw=False)
        if not isinstance(announcement, dict):
            raise ValueError("conteúdo inválido")
    except Exception as error:
        print(f"[{get_timestamp()}][SUB-ERRO] Anúncio inválido: {error}", flush=True)
        return False

    observe_logical_clock(announcement.get("logical_clock"))
    name = announcement.get("coordinator")
    if not isinstance(name, str) or not name or name == SERVER_NAME:
        return False
    print(f"[{get_timestamp()}][SUB] [{SERVERS_TOPIC}] {dict_to_string(announcement)}", flush=True)

    try:
        announced_rank = int(announcement.get("rank"))
    except (TypeError, ValueError):
        announced_rank = None
    if announced_rank is not None and my_rank is not None and announced_rank < my_rank:
        print(f"[{get_timestamp()}][ELEIÇÃO] {name} tem rank menor; reafirmando a liderança", flush=True)
        return True

    coordinator = name
    print(f"[{get_timestamp()}][COORDENADOR] Coordenador atual: {coordinator}", flush=True)
    return False

def handle_peer_request(raw_request: bytes, peer_socket: zmq.Socket) -> bool:
    start_election = False
    try:
        request = msgpack.unpackb(raw_request, raw=False)
        if not isinstance(request, dict):
            raise ValueError("a mensagem não é um mapa")
    except Exception as error:
        print(f"[{get_timestamp()}][PEER-ERRO] Requisição inválida: {error}", flush=True)
        request = None

    if request is None:
        response = {"status": "error", "message": "Requisição inválida"}
    else:
        observe_logical_clock(request.get("logical_clock"))
        print(f"[{get_timestamp()}][PEER-RECEBIDO] {dict_to_string(request)}", flush=True)
        action = request.get("action")
        if action == "clock":
            if coordinator == SERVER_NAME:
                response = {"status": "success", "message": "Hora correta"}
            else:
                response = {"status": "error", "message": "Não sou o coordenador"}
        elif action == "election":
            response = {"status": "success", "message": "OK"}
            try:
                sender_rank = int(request.get("rank"))
            except (TypeError, ValueError):
                sender_rank = None
            start_election = my_rank is not None and sender_rank is not None and sender_rank < my_rank
        else:
            response = {"status": "error", "message": "Ação desconhecida"}

    response["logical_clock"] = tick()
    response["timestamp"] = get_timestamp()
    print(f"[{response['timestamp']}][PEER-ENVIADO] {dict_to_string(response)}", flush=True)
    peer_socket.send(msgpack.packb(response, use_bin_type=True))
    return start_election

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

def handle_client_message(raw_request: bytes, socket: zmq.Socket, publisher: zmq.Socket) -> None:
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

    peer_socket = context.socket(zmq.REP)
    peer_socket.bind(PEER_BIND_ENDPOINT)
    subscriber = context.socket(zmq.SUB)
    subscriber.connect(PUBSUB_XPUB_ENDPOINT)
    subscriber.setsockopt_string(zmq.SUBSCRIBE, SERVERS_TOPIC)
    print(f"[Servidor Python] Escutando servidores em {PEER_BIND_ENDPOINT} e o tópico '{SERVERS_TOPIC}'", flush=True)

    if REFERENCE_ENDPOINT:
        print(f"[Servidor Python] {SERVER_NAME} usando a referência em {REFERENCE_ENDPOINT}", flush=True)
        if clock_offset:
            print(
                f"[Servidor Python] Relógio iniciado com deslocamento simulado de "
                f"{clock_offset.total_seconds() * 1000:+.0f} ms",
                flush=True,
            )
        if request_rank(context) is not None:
            run_election(context, publisher)

    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(peer_socket, zmq.POLLIN)
    poller.register(subscriber, zmq.POLLIN)

    client_messages = 0
    try:
        while True:
            events = dict(poller.poll(500))
            start_election = False

            if peer_socket in events:
                start_election = handle_peer_request(peer_socket.recv(), peer_socket) or start_election

            if subscriber in events:
                start_election = handle_announcement(subscriber.recv_multipart()) or start_election

            if socket in events:
                client_messages += 1
                handle_client_message(socket.recv(), socket, publisher)
                if REFERENCE_ENDPOINT and client_messages % HEARTBEAT_INTERVAL == 0:
                    send_heartbeat(context)
                if REFERENCE_ENDPOINT and client_messages % CLOCK_SYNC_INTERVAL == 0:
                    synchronize_with_coordinator(context, publisher)
            if start_election:
                run_election(context, publisher)
    finally:
        socket.close()
        publisher.close()
        peer_socket.close()
        subscriber.close()
        context.term()

if __name__ == "__main__":
    main()