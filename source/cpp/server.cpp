#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <iomanip>
#include <memory>
#include <set>
#include <stdexcept>
#include <ctime>
#include <filesystem>
#include <unistd.h>
#include <zmq.hpp>
#include <msgpack.hpp>
#include <map>

// Relógio lógico (Lamport) do servidor.
long long logical_clock = 0;
// Diferença, em ms, entre o relógio físico do servidor e o do sistema.
// Começa com CLOCK_OFFSET_MS (atraso simulado) e é corrigida a cada heartbeat.
long long clock_offset_ms = 0;
std::string server_name;
std::string reference_endpoint;
int client_messages = 0;
const int HEARTBEAT_INTERVAL = 10;

long long now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
               .count() +
           clock_offset_ms;
}

std::string get_timestamp() {
    const long long milliseconds = now_ms();
    std::time_t in_time_t = static_cast<std::time_t>(milliseconds / 1000);
    std::tm local_time{};
    localtime_r(&in_time_t, &local_time);
    std::stringstream ss;
    ss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

// Converte "AAAA-MM-DDTHH:MM:SS.mmmZ" (ou com espaço no lugar do T) em ms desde a época.
bool parse_timestamp_ms(const std::string& timestamp, long long& milliseconds) {
    std::tm time{};
    double seconds = 0;
    if (std::sscanf(timestamp.c_str(), "%d-%d-%d%*c%d:%d:%lf", &time.tm_year, &time.tm_mon, &time.tm_mday,
                    &time.tm_hour, &time.tm_min, &seconds) != 6) {
        return false;
    }
    time.tm_year -= 1900;
    time.tm_mon -= 1;
    time.tm_sec = 0;
    milliseconds = static_cast<long long>(timegm(&time)) * 1000 + static_cast<long long>(seconds * 1000);
    return true;
}

std::string trim(const std::string& value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string map_to_string(const std::map<std::string, std::string>& values) {
    std::string output = "{ ";
    bool first = true;
    for (const auto& [key, value] : values) {
        if (!first) output += " | ";
        output += key + ": " + value;
        first = false;
    }
    return output + " }";
}

// As mensagens podem trazer valores de outros tipos além de texto (por exemplo,
// logical_clock é um número e a lista de servidores é um array), então cada valor
// é convertido para texto antes de ser usado.
std::string value_to_string(const msgpack::object& value) {
    switch (value.type) {
        case msgpack::type::STR:
            return std::string(value.via.str.ptr, value.via.str.size);
        case msgpack::type::BIN:
            return std::string(value.via.bin.ptr, value.via.bin.size);
        case msgpack::type::POSITIVE_INTEGER:
            return std::to_string(value.via.u64);
        case msgpack::type::NEGATIVE_INTEGER:
            return std::to_string(value.via.i64);
        case msgpack::type::FLOAT32:
        case msgpack::type::FLOAT64:
            return std::to_string(value.via.f64);
        case msgpack::type::BOOLEAN:
            return value.via.boolean ? "true" : "false";
        case msgpack::type::ARRAY: {
            std::string output;
            for (uint32_t i = 0; i < value.via.array.size; ++i) {
                if (i > 0) output += ";";
                output += value_to_string(value.via.array.ptr[i]);
            }
            return output;
        }
        case msgpack::type::MAP: {
            // Item da lista de servidores: { name, rank } vira "nome:rank".
            std::map<std::string, std::string> fields;
            for (uint32_t i = 0; i < value.via.map.size; ++i) {
                fields[value_to_string(value.via.map.ptr[i].key)] = value_to_string(value.via.map.ptr[i].val);
            }
            if (fields.count("name") && fields.count("rank")) {
                return fields["name"] + ":" + fields["rank"];
            }
            return map_to_string(fields);
        }
        default:
            return "";
    }
}

std::map<std::string, std::string> decode_message(const char* data, std::size_t size) {
    msgpack::object_handle handle = msgpack::unpack(data, size);
    const msgpack::object& object = handle.get();
    if (object.type != msgpack::type::MAP) {
        throw std::runtime_error("a mensagem não é um mapa");
    }
    std::map<std::string, std::string> fields;
    for (uint32_t i = 0; i < object.via.map.size; ++i) {
        fields[value_to_string(object.via.map.ptr[i].key)] = value_to_string(object.via.map.ptr[i].val);
    }
    return fields;
}

// Empacota os campos de texto e o logical_clock como número inteiro.
msgpack::sbuffer encode_message(std::map<std::string, std::string> fields, long long clock) {
    fields.erase("logical_clock");
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_map(static_cast<uint32_t>(fields.size() + 1));
    for (const auto& [key, value] : fields) {
        packer.pack(key);
        packer.pack(value);
    }
    packer.pack(std::string("logical_clock"));
    packer.pack(clock);
    return buffer;
}

// Incrementa o relógio lógico antes de enviar uma mensagem.
long long tick() {
    return ++logical_clock;
}

// Ao receber, fica com o maior valor entre o local e o recebido.
void update_clock(const std::map<std::string, std::string>& message) {
    const auto clock = message.find("logical_clock");
    if (clock == message.end()) return;
    try {
        logical_clock = std::max(logical_clock, std::stoll(clock->second));
    } catch (const std::exception&) {
    }
}

void save_user(const std::string& username, const std::string& timestamp) {
    std::ofstream file("data/users.txt", std::ios::app);
    file << username << "," << timestamp << "\n";
}

void save_channel(const std::string& channel) {
    std::ofstream file("data/channels.txt", std::ios::app);
    file << channel << "\n";
}

void save_message(const std::string& channel, const std::string& username, const std::string& message,
                  const std::string& request_timestamp, const std::string& timestamp) {
    std::ofstream file("data/messages.txt", std::ios::app);
    file << channel << "|" << username << "|" << message << "|" << request_timestamp << "|" << timestamp << "\n";
}

std::set<std::string> load_channels() {
    std::set<std::string> channels;
    std::ifstream file("data/channels.txt");
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty()) channels.insert(line);
    }
    return channels;
}

// Socket REQ para o serviço de referência. Se a referência não responder dentro
// do timeout, o socket é recriado (um REQ sem resposta fica travado).
std::unique_ptr<zmq::socket_t> reference_socket;

void connect_reference(zmq::context_t& context) {
    reference_socket = std::make_unique<zmq::socket_t>(context, zmq::socket_type::req);
    reference_socket->set(zmq::sockopt::rcvtimeo, 2000);
    reference_socket->set(zmq::sockopt::linger, 0);
    reference_socket->connect(reference_endpoint);
}

bool reference_request(zmq::context_t& context,
                       std::map<std::string, std::string> request,
                       std::map<std::string, std::string>& response) {
    request["name"] = server_name;
    request["timestamp"] = get_timestamp();
    const long long clock = tick();
    std::cout << "[" << request["timestamp"] << "][REQ REFERÊNCIA] " << map_to_string(request)
              << " | logical_clock: " << clock << std::endl;

    msgpack::sbuffer buffer = encode_message(request, clock);
    zmq::message_t message(buffer.data(), buffer.size());
    reference_socket->send(message, zmq::send_flags::none);

    zmq::message_t reply;
    if (!reference_socket->recv(reply, zmq::recv_flags::none)) {
        std::cerr << "[" << get_timestamp() << "][REFERÊNCIA ERRO] Sem resposta, reconectando" << std::endl;
        connect_reference(context);
        return false;
    }
    try {
        response = decode_message(static_cast<const char*>(reply.data()), reply.size());
    } catch (const std::exception& error) {
        std::cerr << "[" << get_timestamp() << "][REFERÊNCIA ERRO] Resposta inválida: " << error.what() << std::endl;
        return false;
    }
    update_clock(response);
    std::cout << "[" << get_timestamp() << "][REP REFERÊNCIA] " << map_to_string(response) << std::endl;
    return response["status"] == "success";
}

void request_rank(zmq::context_t& context) {
    while (true) {
        std::map<std::string, std::string> response;
        if (reference_request(context, {{"action", "rank"}}, response)) {
            std::cout << "[" << get_timestamp() << "][RANK] " << server_name << " tem rank "
                      << response["rank"] << std::endl;
            return;
        }
        sleep(1);
    }
}

// Heartbeat: mantém o servidor na lista da referência e aproveita a resposta
// para acertar o relógio físico (algoritmo de Cristian: hora da referência +
// metade do tempo de ida e volta).
void send_heartbeat(zmq::context_t& context) {
    std::map<std::string, std::string> response;
    const long long sent_at = now_ms();
    if (!reference_request(context, {{"action", "heartbeat"}}, response)) {
        // A referência pode ter esquecido o servidor (por exemplo, se reiniciou).
        if (response["message"] == "Servidor não registrado") {
            request_rank(context);
        }
        return;
    }
    const long long received_at = now_ms();

    long long reference_time = 0;
    if (parse_timestamp_ms(response["timestamp"], reference_time)) {
        const long long estimated_time = reference_time + (received_at - sent_at) / 2;
        const long long adjustment = estimated_time - received_at;
        clock_offset_ms += adjustment;
        std::cout << "[" << get_timestamp() << "][RELÓGIO] Ajuste de " << adjustment
                  << " ms (diferença atual para o sistema: " << clock_offset_ms << " ms)" << std::endl;
    } else {
        std::cerr << "[" << get_timestamp() << "][RELÓGIO ERRO] Hora inválida na resposta" << std::endl;
    }

    std::map<std::string, std::string> servers;
    if (reference_request(context, {{"action", "list"}}, servers)) {
        std::cout << "[" << get_timestamp() << "][SERVIDORES] " << servers["servers"] << std::endl;
    }
}

std::map<std::string, std::string> handle_request(std::map<std::string, std::string>& req_map,
                                                  zmq::socket_t& pub_socket) {
    std::string action = req_map["action"];
    std::string request_timestamp = req_map["timestamp"];
    std::map<std::string, std::string> rep_map;

    if (request_timestamp.empty()) {
        rep_map["status"] = "error";
        rep_map["message"] = "Timestamp ausente";
        std::cout << "[" << get_timestamp() << "][ERRO] Timestamp ausente" << std::endl;
    }
    else if (action == "login") {
        std::string username = trim(req_map["username"]);
        if (username.empty()) {
            rep_map["status"] = "error";
            rep_map["message"] = "Nome de usuário inválido";
            std::cout << "[" << request_timestamp << "][LOGIN ERRO] Nome inválido" << std::endl;
        } else {
            save_user(username, request_timestamp);
            std::cout << "[" << request_timestamp << "][LOGIN] Usuário: " << username << std::endl;
            rep_map["status"] = "success";
            rep_map["message"] = "Login realizado com sucesso";
        }
    }
    else if (action == "create_channel") {
        std::string channel = trim(req_map["channel"]);
        auto channels = load_channels();
        if (channel.empty()) {
            rep_map["status"] = "error";
            rep_map["message"] = "Nome de canal inválido";
            std::cout << "[" << request_timestamp << "][CANAL ERRO] Nome inválido" << std::endl;
        } else if (channels.find(channel) == channels.end()) {
            save_channel(channel);
            std::cout << "[" << request_timestamp << "][CHANNEL] Canal criado: " << channel << std::endl;
            rep_map["status"] = "success";
            rep_map["message"] = "Canal criado com sucesso";
        } else {
            rep_map["status"] = "error";
            rep_map["message"] = "Canal já existe";
            std::cout << "[" << request_timestamp << "][CANAL ERRO] Já existe: " << channel << std::endl;
        }
    }
    else if (action == "list_channels") {
        auto channels = load_channels();
        std::string channel_list;
        for (const auto& channel : channels) {
            if (!channel_list.empty()) channel_list += ";";
            channel_list += channel;
        }
        rep_map["status"] = "success";
        rep_map["channels"] = channel_list;
        std::cout << "[" << request_timestamp << "][CANAIS] Lista enviada: " << channel_list << std::endl;
    }
    else if (action == "publish") {
        std::string channel = trim(req_map["channel"]);
        std::string message = req_map["message"];
        std::string username = req_map["username"];

        auto channels = load_channels();
        if (channel.empty() || trim(message).empty()) {
            rep_map["status"] = "error";
            rep_map["message"] = "Canal ou mensagem inválidos";
            std::cout << "[" << request_timestamp << "][PUB ERRO] Canal ou mensagem inválidos" << std::endl;
        } else {
            // Com o broker, o canal pode ter sido criado em outro servidor;
            // ele passa a ser registrado também neste.
            if (channels.find(channel) == channels.end()) {
                save_channel(channel);
                std::cout << "[" << request_timestamp << "][CHANNEL] Canal registrado a partir de publicação: " << channel << std::endl;
            }
            const std::string publication_timestamp = get_timestamp();
            save_message(channel, username, message, request_timestamp, publication_timestamp);

            std::map<std::string, std::string> publication = {
                {"channel", channel},
                {"message", message},
                {"timestamp", publication_timestamp},
                {"username", username}
            };
            const long long clock = tick();
            msgpack::sbuffer publication_buffer = encode_message(publication, clock);
            zmq::message_t topic_msg(channel.data(), channel.size());
            zmq::message_t content_msg(publication_buffer.data(), publication_buffer.size());

            pub_socket.send(topic_msg, zmq::send_flags::sndmore);
            pub_socket.send(content_msg, zmq::send_flags::none);

            rep_map["status"] = "success";
            rep_map["message"] = "Mensagem publicada com sucesso";
            std::cout << "[" << publication_timestamp << "][PUB] " << map_to_string(publication)
                      << " | logical_clock: " << clock << std::endl;
        }
    }
    else {
        rep_map["status"] = "error";
        rep_map["message"] = "Ação desconhecida";
        std::cout << "[" << request_timestamp << "][ERRO] Ação desconhecida: " << action << std::endl;
    }

    return rep_map;
}

int main() {
    std::filesystem::create_directories("data");

    char hostname[256] = {};
    gethostname(hostname, sizeof(hostname) - 1);
    const char* configured_name = std::getenv("SERVER_NAME");
    server_name = configured_name ? configured_name : hostname;
    // Sem REFERENCE_ENDPOINT o servidor funciona sem o serviço de referência.
    const char* configured_reference = std::getenv("REFERENCE_ENDPOINT");
    reference_endpoint = configured_reference ? configured_reference : "";
    const char* configured_offset = std::getenv("CLOCK_OFFSET_MS");
    if (configured_offset) {
        clock_offset_ms = std::atoll(configured_offset);
    }

    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::rep);
    zmq::socket_t pub_socket(context, zmq::socket_type::pub);

    // Com BROKER_BACKEND definido, o servidor conecta no DEALER do broker;
    // sem ele, atende os clientes diretamente na porta 5555.
    const char* broker_backend = std::getenv("BROKER_BACKEND");
    if (broker_backend) {
        socket.connect(broker_backend);
        std::cout << "[Servidor C++] " << server_name << " conectado ao broker em " << broker_backend << std::endl;
    } else {
        socket.bind("tcp://*:5555");
        std::cout << "[Servidor C++] " << server_name << " iniciado na porta 5555..." << std::endl;
    }

    const char* pubsub_xsub = std::getenv("PUBSUB_XSUB_ENDPOINT");
    pub_socket.connect(pubsub_xsub ? pubsub_xsub : "tcp://proxy-pubsub:5557");
    std::cout << "[Servidor C++] Publisher conectado ao proxy Pub/Sub" << std::endl;
    if (clock_offset_ms != 0) {
        std::cout << "[" << get_timestamp() << "][RELÓGIO] Iniciando com atraso simulado de "
                  << clock_offset_ms << " ms" << std::endl;
    }

    if (!reference_endpoint.empty()) {
        connect_reference(context);
        request_rank(context);
    }

    while (true) {
        zmq::message_t request;
        if (!socket.recv(request, zmq::recv_flags::none)) {
            continue;
        }

        std::map<std::string, std::string> rep_map;
        try {
            std::map<std::string, std::string> req_map =
                decode_message(static_cast<const char*>(request.data()), request.size());
            update_clock(req_map);
            std::cout << "[" << get_timestamp() << "][RECEBIDO] " << map_to_string(req_map) << std::endl;
            rep_map = handle_request(req_map, pub_socket);
        } catch (const std::exception& error) {
            rep_map["status"] = "error";
            rep_map["message"] = "Requisição inválida";
            std::cerr << "[" << get_timestamp() << "][ERRO] Requisição inválida: " << error.what() << std::endl;
        }

        rep_map["timestamp"] = get_timestamp();
        const long long clock = tick();
        std::cout << "[" << rep_map["timestamp"] << "][ENVIADO] " << map_to_string(rep_map)
                  << " | logical_clock: " << clock << std::endl;

        msgpack::sbuffer sbuf = encode_message(rep_map, clock);
        zmq::message_t reply(sbuf.data(), sbuf.size());
        socket.send(reply, zmq::send_flags::none);

        // A cada 10 mensagens de clientes, avisa a referência que está ativo.
        client_messages++;
        if (!reference_endpoint.empty() && client_messages % HEARTBEAT_INTERVAL == 0) {
            send_heartbeat(context);
        }
    }

    return 0;
}
