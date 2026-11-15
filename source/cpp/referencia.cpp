#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <zmq.hpp>
#include <msgpack.hpp>

// Serviço de referência (compatível com o reference.js): só conversa com os servidores.
// - rank: devolve o rank do servidor (o mesmo nome recebe sempre o mesmo rank)
// - heartbeat: mantém o servidor na lista e responde só OK ("Servidor não registrado" se
//   ele não pediu rank). Desde a parte 4 a hora correta vem do coordenador, então o
//   heartbeat não responde mais com a hora.
// - list: devolve os servidores disponíveis como lista de { name, rank }
// Toda resposta leva o timestamp de envio e o logical_clock, como qualquer mensagem.

struct ServerInfo {
    long long rank;
    std::chrono::steady_clock::time_point last_seen;
    bool available;
};

std::map<std::string, ServerInfo> servers;
long long next_rank = 1;
long long logical_clock = 0;
long long heartbeat_timeout_ms = 30000;

// Horário local, usado só nos logs.
std::string get_log_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm local_time{};
    localtime_r(&in_time_t, &local_time);
    std::stringstream ss;
    ss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

// Timestamp de envio das respostas: "AAAA-MM-DDTHH:MM:SS.mmmZ".
std::string get_iso_timestamp() {
    const auto now = std::chrono::system_clock::now();
    const auto milliseconds =
        std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
    std::time_t in_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm utc_time{};
    gmtime_r(&in_time_t, &utc_time);
    std::stringstream ss;
    ss << std::put_time(&utc_time, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(3) << std::setfill('0')
       << milliseconds << "Z";
    return ss.str();
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

// Relógio lógico: ao receber, fica com o maior valor entre o local e o recebido.
void update_clock(const std::map<std::string, std::string>& message) {
    const auto clock = message.find("logical_clock");
    if (clock == message.end()) return;
    try {
        logical_clock = std::max(logical_clock, std::stoll(clock->second));
    } catch (const std::exception&) {
    }
}

// Resposta da referência. rank e servers só são enviados quando preenchidos.
struct Response {
    std::string status;
    std::string message;
    long long rank = 0;
    bool has_servers = false;
    std::vector<std::pair<std::string, long long>> server_list;
};

std::string response_to_string(const Response& response, const std::string& timestamp, long long clock) {
    std::map<std::string, std::string> fields = {{"status", response.status}, {"timestamp", timestamp},
                                                 {"logical_clock", std::to_string(clock)}};
    if (!response.message.empty()) fields["message"] = response.message;
    if (response.rank > 0) fields["rank"] = std::to_string(response.rank);
    if (response.has_servers) {
        std::string list;
        for (const auto& [name, rank] : response.server_list) {
            if (!list.empty()) list += ";";
            list += name + ":" + std::to_string(rank);
        }
        fields["servers"] = list;
    }
    return map_to_string(fields);
}

msgpack::sbuffer encode_response(const Response& response, const std::string& timestamp, long long clock) {
    uint32_t size = 3;
    if (!response.message.empty()) size++;
    if (response.rank > 0) size++;
    if (response.has_servers) size++;

    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_map(size);
    packer.pack(std::string("status"));
    packer.pack(response.status);
    if (!response.message.empty()) {
        packer.pack(std::string("message"));
        packer.pack(response.message);
    }
    if (response.rank > 0) {
        packer.pack(std::string("rank"));
        packer.pack(response.rank);
    }
    if (response.has_servers) {
        packer.pack(std::string("servers"));
        packer.pack_array(static_cast<uint32_t>(response.server_list.size()));
        for (const auto& [name, rank] : response.server_list) {
            packer.pack_map(2);
            packer.pack(std::string("name"));
            packer.pack(name);
            packer.pack(std::string("rank"));
            packer.pack(rank);
        }
    }
    packer.pack(std::string("timestamp"));
    packer.pack(timestamp);
    packer.pack(std::string("logical_clock"));
    packer.pack(clock);
    return buffer;
}

void remove_expired_servers() {
    const auto now = std::chrono::steady_clock::now();
    for (auto& [name, info] : servers) {
        if (info.available &&
            std::chrono::duration_cast<std::chrono::milliseconds>(now - info.last_seen).count() > heartbeat_timeout_ms) {
            info.available = false;
            std::cout << "[" << get_log_timestamp() << "][REMOVIDO] " << name << " (rank " << info.rank
                      << ") sem heartbeat há mais de " << heartbeat_timeout_ms / 1000 << "s" << std::endl;
        }
    }
}

Response handle_request(std::map<std::string, std::string>& request) {
    const std::string action = request["action"];
    const std::string name = trim(request["name"]);
    Response response;

    if ((action == "rank" || action == "heartbeat") && name.empty()) {
        response.status = "error";
        response.message = "Nome do servidor inválido";
    } else if (action == "rank") {
        auto it = servers.find(name);
        if (it == servers.end()) {
            it = servers.emplace(name, ServerInfo{next_rank++, {}, false}).first;
            std::cout << "[" << get_log_timestamp() << "][REGISTRO] " << name << " recebeu o rank "
                      << it->second.rank << std::endl;
        }
        it->second.last_seen = std::chrono::steady_clock::now();
        it->second.available = true;
        response.status = "success";
        response.rank = it->second.rank;
    } else if (action == "heartbeat") {
        auto it = servers.find(name);
        if (it == servers.end()) {
            response.status = "error";
            response.message = "Servidor não registrado";
        } else {
            if (!it->second.available) {
                std::cout << "[" << get_log_timestamp() << "][REGISTRO] " << name
                          << " voltou para a lista (rank " << it->second.rank << ")" << std::endl;
            }
            it->second.last_seen = std::chrono::steady_clock::now();
            it->second.available = true;
            response.status = "success";
            response.message = "OK";
        }
    } else if (action == "list") {
        response.status = "success";
        response.has_servers = true;
        for (const auto& [server_name, info] : servers) {
            if (info.available) response.server_list.emplace_back(server_name, info.rank);
        }
        std::sort(response.server_list.begin(), response.server_list.end(),
                  [](const auto& left, const auto& right) { return left.second < right.second; });
    } else {
        response.status = "error";
        response.message = "Ação desconhecida";
    }
    return response;
}

int main() {
    const char* timeout = std::getenv("HEARTBEAT_TIMEOUT_MS");
    if (timeout) {
        heartbeat_timeout_ms = std::atoll(timeout);
    }
    const char* endpoint = std::getenv("REFERENCE_BIND_ENDPOINT");

    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::rep);
    socket.bind(endpoint ? endpoint : "tcp://*:5559");
    std::cout << "[Referência C++] Ativa em " << (endpoint ? endpoint : "tcp://*:5559")
              << " (timeout de heartbeat: " << heartbeat_timeout_ms / 1000 << "s)" << std::endl;

    while (true) {
        // Timeout para conseguir remover servidores mesmo sem receber mensagens.
        zmq::pollitem_t items[] = {{socket.handle(), 0, ZMQ_POLLIN, 0}};
        zmq::poll(items, 1, std::chrono::milliseconds(1000));
        remove_expired_servers();
        if (!(items[0].revents & ZMQ_POLLIN)) {
            continue;
        }

        zmq::message_t request;
        if (!socket.recv(request, zmq::recv_flags::none)) {
            continue;
        }

        Response response;
        try {
            std::map<std::string, std::string> fields =
                decode_message(static_cast<const char*>(request.data()), request.size());
            update_clock(fields);
            std::cout << "[" << get_log_timestamp() << "][RECEBIDO] " << map_to_string(fields) << std::endl;
            response = handle_request(fields);
        } catch (const std::exception& error) {
            response.status = "error";
            response.message = "Requisição inválida";
            std::cerr << "[" << get_log_timestamp() << "][ERRO] Requisição inválida: " << error.what() << std::endl;
        }

        const long long clock = ++logical_clock;
        const std::string timestamp = get_iso_timestamp();
        std::cout << "[" << get_log_timestamp() << "][ENVIADO] " << response_to_string(response, timestamp, clock)
                  << std::endl;

        msgpack::sbuffer buffer = encode_response(response, timestamp, clock);
        zmq::message_t reply(buffer.data(), buffer.size());
        socket.send(reply, zmq::send_flags::none);
    }

    return 0;
}
