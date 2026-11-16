#include <iostream>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iomanip>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <ctime>
#include <filesystem>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>
#include <msgpack.hpp>
#include <map>

// Relógio lógico (Lamport) do servidor. A thread principal e a thread que atende
// os outros servidores mexem nele, então fica protegido por mutex.
std::mutex clock_mutex;
long long logical_clock = 0;
// Diferença, em ms, entre o relógio físico do servidor e o do sistema.
// Começa com CLOCK_OFFSET_MS (atraso simulado) e é corrigida pela sincronização
// com o coordenador (algoritmo de Berkeley).
std::atomic<long long> clock_offset_ms(0);

std::string server_name;
std::string reference_endpoint;
int peer_port = 5560;
int my_rank = 0;
int client_messages = 0;
const int HEARTBEAT_INTERVAL = 10;
const int CLOCK_SYNC_INTERVAL = 15;

// Estado da eleição. "coordinator" é o nome do servidor eleito como coordenador.
std::mutex state_mutex;
std::string coordinator;
std::atomic<bool> election_requested(false);
// Servidores disponíveis segundo a referência: nome e rank.
std::vector<std::pair<std::string, int>> known_servers;

// As duas threads escrevem na tela; o mutex evita que as linhas se misturem.
std::mutex output_mutex;

template <typename... Args>
void log_line(Args&&... args) {
    std::ostringstream text;
    (text << ... << args);
    std::lock_guard<std::mutex> lock(output_mutex);
    std::cout << text.str() << std::endl;
}

template <typename... Args>
void log_error(Args&&... args) {
    std::ostringstream text;
    (text << ... << args);
    std::lock_guard<std::mutex> lock(output_mutex);
    std::cerr << text.str() << std::endl;
}

long long now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
               .count() +
           clock_offset_ms.load();
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

// Hora em ms -> "AAAA-MM-DDTHH:MM:SS.mmmZ" (UTC). É o formato que Python e JS usam
// para trocar a hora correta com o coordenador.
std::string iso_timestamp(long long milliseconds) {
    std::time_t in_time_t = static_cast<std::time_t>(milliseconds / 1000);
    std::tm utc_time{};
    gmtime_r(&in_time_t, &utc_time);
    std::stringstream ss;
    ss << std::put_time(&utc_time, "%Y-%m-%dT%H:%M:%S") << "." << std::setw(3) << std::setfill('0')
       << (milliseconds % 1000) << "Z";
    return ss.str();
}

// "AAAA-MM-DDTHH:MM:SS(.fff)" (com T ou espaço; considerado UTC) -> ms desde a época.
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

// Hora informada numa mensagem de relógio: o campo "time" (ms) se existir;
// senão, o "timestamp" (como mandam os servidores Python e JS).
bool message_time_ms(const std::map<std::string, std::string>& fields, long long& milliseconds) {
    const auto time = fields.find("time");
    if (time != fields.end() && !time->second.empty()) {
        try {
            milliseconds = std::stoll(time->second);
            return true;
        } catch (const std::exception&) {
        }
    }
    const auto timestamp = fields.find("timestamp");
    return timestamp != fields.end() && parse_timestamp_ms(timestamp->second, milliseconds);
}

std::string get_coordinator() {
    std::lock_guard<std::mutex> lock(state_mutex);
    return coordinator;
}

void set_coordinator(const std::string& name) {
    std::lock_guard<std::mutex> lock(state_mutex);
    coordinator = name;
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
    std::lock_guard<std::mutex> lock(clock_mutex);
    return ++logical_clock;
}

// Ao receber, fica com o maior valor entre o local e o recebido.
void update_clock(const std::map<std::string, std::string>& message) {
    const auto clock = message.find("logical_clock");
    if (clock == message.end()) return;
    try {
        const long long received = std::stoll(clock->second);
        std::lock_guard<std::mutex> lock(clock_mutex);
        logical_clock = std::max(logical_clock, received);
    } catch (const std::exception&) {
    }
}

void send_message(zmq::socket_t& socket, const std::map<std::string, std::string>& fields, long long clock) {
    msgpack::sbuffer buffer = encode_message(fields, clock);
    zmq::message_t message(buffer.data(), buffer.size());
    socket.send(message, zmq::send_flags::none);
}

// ---------------------------------------------------------------------------
// Persistência
// ---------------------------------------------------------------------------
// Com a replicação, o mesmo dado pode chegar mais de uma vez (pelo tópico
// "replication" e pela sincronização inicial), então cada arquivo só recebe
// linhas que ainda não tem. A thread que atende os outros servidores também lê
// esses arquivos, por isso o acesso é protegido por mutex.
const char* USERS_FILE = "data/users.txt";
const char* CHANNELS_FILE = "data/channels.txt";
const char* MESSAGES_FILE = "data/messages.txt";
std::mutex data_mutex;
std::map<std::string, std::set<std::string>> stored_lines;  // arquivo -> linhas já gravadas

std::vector<std::string> read_lines(const std::string& path) {
    std::vector<std::string> lines;
    std::ifstream file(path);
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty()) lines.push_back(line);
    }
    return lines;
}

void load_stored_lines() {
    std::lock_guard<std::mutex> lock(data_mutex);
    for (const char* path : {USERS_FILE, CHANNELS_FILE, MESSAGES_FILE}) {
        for (const auto& line : read_lines(path)) {
            stored_lines[path].insert(line);
        }
    }
}

// Grava a linha no arquivo se ela ainda não existir. Devolve true se gravou.
bool append_line(const char* path, const std::string& line) {
    std::lock_guard<std::mutex> lock(data_mutex);
    if (!stored_lines[path].insert(line).second) {
        return false;
    }
    std::ofstream file(path, std::ios::app);
    file << line << "\n";
    return true;
}

std::string user_line(const std::string& username, const std::string& timestamp) {
    return username + "," + timestamp;
}

std::string message_line(const std::string& channel, const std::string& username, const std::string& message,
                         const std::string& request_timestamp, const std::string& timestamp) {
    return channel + "|" + username + "|" + message + "|" + request_timestamp + "|" + timestamp;
}

bool save_user(const std::string& username, const std::string& timestamp) {
    return append_line(USERS_FILE, user_line(username, timestamp));
}

bool save_channel(const std::string& channel) {
    return append_line(CHANNELS_FILE, channel);
}

bool save_message(const std::string& channel, const std::string& username, const std::string& message,
                  const std::string& request_timestamp, const std::string& timestamp) {
    return append_line(MESSAGES_FILE, message_line(channel, username, message, request_timestamp, timestamp));
}

std::set<std::string> load_channels() {
    std::lock_guard<std::mutex> lock(data_mutex);
    return stored_lines[CHANNELS_FILE];
}

// Todas as linhas de um arquivo, separadas por quebra de linha (usado na sincronização inicial).
std::string dump_lines(const char* path) {
    std::lock_guard<std::mutex> lock(data_mutex);
    std::string output;
    for (const auto& line : stored_lines[path]) {
        output += line + "\n";
    }
    return output;
}

// Grava as linhas recebidas (separadas por quebra de linha) e devolve quantas eram novas.
int merge_lines(const char* path, const std::string& lines) {
    int added = 0;
    std::stringstream stream(lines);
    std::string line;
    while (std::getline(stream, line)) {
        if (!line.empty() && append_line(path, line)) added++;
    }
    return added;
}

// ---------------------------------------------------------------------------
// Replicação (parte 5)
// ---------------------------------------------------------------------------
// Replicação ativa pelo Pub/Sub: toda gravação feita a partir de um cliente
// (login, criação de canal ou publicação) é publicada no tópico "replication".
// Todos os servidores assinam o tópico e gravam o mesmo dado no próprio disco,
// então todos acabam com todos os dados.
const std::string REPLICATION_TOPIC = "replication";
std::string replication_prefix;  // nome do servidor + horário de início (ids não se repetem após reiniciar)
long long replication_sequence = 0;
std::set<std::string> applied_replications;  // ids já aplicados (só a thread principal usa)

void publish_replication(zmq::socket_t& pub_socket, std::map<std::string, std::string> fields) {
    fields["origin"] = server_name;
    fields["id"] = replication_prefix + ":" + std::to_string(++replication_sequence);
    fields["timestamp"] = get_timestamp();
    const long long clock = tick();
    msgpack::sbuffer buffer = encode_message(fields, clock);
    zmq::message_t topic_msg(REPLICATION_TOPIC.data(), REPLICATION_TOPIC.size());
    zmq::message_t content_msg(buffer.data(), buffer.size());
    pub_socket.send(topic_msg, zmq::send_flags::sndmore);
    pub_socket.send(content_msg, zmq::send_flags::none);
    log_line("[", fields["timestamp"], "][REPLICAÇÃO] Enviado: ", map_to_string(fields), " | logical_clock: ", clock);
}

void apply_replication(const std::map<std::string, std::string>& fields) {
    auto get = [&](const std::string& key) {
        const auto it = fields.find(key);
        return it == fields.end() ? std::string() : it->second;
    };
    const std::string origin = get("origin");
    const std::string id = get("id");
    // O próprio servidor também recebe o que publicou; isso já está gravado.
    if (origin == server_name) return;
    if (!id.empty() && !applied_replications.insert(id).second) return;

    const std::string type = get("type");
    bool stored = false;
    if (type == "login") {
        stored = save_user(get("username"), get("login_timestamp"));
    } else if (type == "channel") {
        stored = save_channel(get("channel"));
    } else if (type == "publication") {
        save_channel(get("channel"));
        stored = save_message(get("channel"), get("username"), get("message"),
                              get("request_timestamp"), get("publication_timestamp"));
    } else {
        log_error("[", get_timestamp(), "][REPLICAÇÃO] Tipo desconhecido de ", origin, ": ", type);
        return;
    }
    log_line("[", get_timestamp(), "][REPLICAÇÃO] Recebido de ", origin, " (", type, ", ",
             stored ? "gravado" : "já existia", "): ", map_to_string(fields));
}

// "nome:rank;nome:rank" -> lista de (nome, rank).
std::vector<std::pair<std::string, int>> parse_servers(const std::string& value) {
    std::vector<std::pair<std::string, int>> servers;
    std::stringstream stream(value);
    std::string item;
    while (std::getline(stream, item, ';')) {
        const auto separator = item.rfind(':');
        if (separator == std::string::npos) continue;
        try {
            servers.emplace_back(item.substr(0, separator), std::stoi(item.substr(separator + 1)));
        } catch (const std::exception&) {
        }
    }
    return servers;
}

// ---------------------------------------------------------------------------
// Serviço de referência (rank, heartbeat e lista de servidores)
// ---------------------------------------------------------------------------

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
    log_line("[", request["timestamp"], "][REQ REFERÊNCIA] ", map_to_string(request), " | logical_clock: ", clock);
    send_message(*reference_socket, request, clock);

    zmq::message_t reply;
    if (!reference_socket->recv(reply, zmq::recv_flags::none)) {
        log_error("[", get_timestamp(), "][REFERÊNCIA ERRO] Sem resposta, reconectando");
        connect_reference(context);
        return false;
    }
    try {
        response = decode_message(static_cast<const char*>(reply.data()), reply.size());
    } catch (const std::exception& error) {
        log_error("[", get_timestamp(), "][REFERÊNCIA ERRO] Resposta inválida: ", error.what());
        return false;
    }
    update_clock(response);
    log_line("[", get_timestamp(), "][REP REFERÊNCIA] ", map_to_string(response));
    return response["status"] == "success";
}

void request_rank(zmq::context_t& context) {
    while (true) {
        std::map<std::string, std::string> response;
        if (reference_request(context, {{"action", "rank"}}, response)) {
            try {
                my_rank = std::stoi(response["rank"]);
            } catch (const std::exception&) {
            }
            log_line("[", get_timestamp(), "][RANK] ", server_name, " tem rank ", my_rank);
            return;
        }
        sleep(1);
    }
}

// Atualiza a lista de servidores disponíveis pela referência.
void request_server_list(zmq::context_t& context) {
    std::map<std::string, std::string> response;
    if (reference_request(context, {{"action", "list"}}, response)) {
        known_servers = parse_servers(response["servers"]);
        log_line("[", get_timestamp(), "][SERVIDORES] ", response["servers"]);
    }
}

// Heartbeat: mantém o servidor na lista da referência. A partir da parte 4 a hora
// correta vem do coordenador, então a resposta do heartbeat não é usada para o relógio.
void send_heartbeat(zmq::context_t& context) {
    std::map<std::string, std::string> response;
    if (!reference_request(context, {{"action", "heartbeat"}}, response)) {
        // A referência pode ter esquecido o servidor (por exemplo, se reiniciou).
        if (response["message"] == "Servidor não registrado") {
            request_rank(context);
        }
        return;
    }
    request_server_list(context);

    // Se o coordenador saiu da lista da referência, é preciso eleger outro.
    const std::string current = get_coordinator();
    if (!current.empty() &&
        std::none_of(known_servers.begin(), known_servers.end(),
                     [&](const auto& server) { return server.first == current; })) {
        log_line("[", get_timestamp(), "][ELEIÇÃO] Coordenador ", current, " não está mais na lista");
        set_coordinator("");
        election_requested = true;
    }
}

// ---------------------------------------------------------------------------
// Comunicação entre servidores (eleição e relógio)
// ---------------------------------------------------------------------------

// Diferenças de relógio (em ms) informadas pelos servidores ao coordenador,
// usadas no algoritmo de Berkeley. Só a thread dos servidores mexe nisso.
struct ClockReport {
    long long difference_ms;
    std::chrono::steady_clock::time_point received_at;
};
std::map<std::string, ClockReport> clock_reports;

// Algoritmo de Berkeley no coordenador: a hora correta é a média dos relógios
// conhecidos (o do coordenador e os informados pelos servidores nos últimos 60 s).
// O coordenador também ajusta o próprio relógio para essa média.
long long berkeley_correct_time(const std::string& requester, long long requester_time) {
    const long long coordinator_time = now_ms();
    const auto now = std::chrono::steady_clock::now();
    clock_reports[requester] = {requester_time - coordinator_time, now};

    for (auto it = clock_reports.begin(); it != clock_reports.end();) {
        if (now - it->second.received_at > std::chrono::seconds(60)) {
            it = clock_reports.erase(it);
        } else {
            ++it;
        }
    }

    long long sum = 0;  // a diferença do próprio coordenador é 0
    for (const auto& [name, report] : clock_reports) {
        sum += report.difference_ms;
    }
    const long long average = sum / static_cast<long long>(clock_reports.size() + 1);

    // O coordenador anda "average" ms; as diferenças guardadas passam a ser relativas
    // ao novo relógio. Quem pediu vai acertar o relógio para a hora correta (diferença 0).
    clock_offset_ms += average;
    for (auto& [name, report] : clock_reports) {
        report.difference_ms -= average;
    }
    clock_reports[requester].difference_ms = 0;

    log_line("[", get_timestamp(), "][RELÓGIO] Coordenador (Berkeley): média de ", clock_reports.size() + 1,
             " relógios; ajuste do coordenador: ", average, " ms; hora enviada a ", requester);
    return coordinator_time + average;
}

// Thread que atende os pedidos de outros servidores (eleição e relógio) num socket REP
// próprio, para não depender da thread principal, que pode estar esperando outro servidor.
void peer_loop(zmq::context_t& context) {
    zmq::socket_t socket(context, zmq::socket_type::rep);
    socket.bind("tcp://*:" + std::to_string(peer_port));
    log_line("[Servidor C++] Atendendo outros servidores na porta ", peer_port);

    while (true) {
        zmq::message_t request;
        if (!socket.recv(request, zmq::recv_flags::none)) {
            continue;
        }

        std::map<std::string, std::string> reply;
        try {
            std::map<std::string, std::string> fields =
                decode_message(static_cast<const char*>(request.data()), request.size());
            update_clock(fields);
            log_line("[", get_timestamp(), "][RECEBIDO SERVIDOR] ", map_to_string(fields));
            const std::string action = fields["action"];

            if (action == "election") {
                // Quem recebe o pedido responde OK e começa a sua própria eleição.
                reply["status"] = "success";
                reply["message"] = "OK";
                election_requested = true;
                log_line("[", get_timestamp(), "][ELEIÇÃO] Pedido de ", fields["name"], " (rank ",
                         fields["rank"], "); respondendo OK");
            } else if (action == "clock") {
                const std::string current = get_coordinator();
                if (current != server_name) {
                    reply["status"] = "error";
                    reply["message"] = "Não sou o coordenador";
                    reply["coordinator"] = current;
                } else {
                    // Sem hora válida no pedido, o relógio de quem pediu entra na média como igual ao do coordenador.
                    long long requester_time = 0;
                    if (!message_time_ms(fields, requester_time)) {
                        requester_time = now_ms();
                    }
                    const long long correct_time = berkeley_correct_time(fields["name"], requester_time);
                    reply["status"] = "success";
                    reply["message"] = "Hora correta";
                    reply["time"] = std::to_string(correct_time);
                    // Python e JS leem a hora correta do timestamp, em ISO com milissegundos.
                    reply["timestamp"] = iso_timestamp(correct_time);
                    reply["coordinator"] = server_name;
                }
            } else if (action == "sync") {
                // Sincronização inicial (parte 5): devolve todos os dados deste servidor.
                reply["status"] = "success";
                reply["users"] = dump_lines(USERS_FILE);
                reply["channels"] = dump_lines(CHANNELS_FILE);
                reply["messages"] = dump_lines(MESSAGES_FILE);
                log_line("[", get_timestamp(), "][REPLICAÇÃO] Enviando todos os dados para ", fields["name"]);
            } else {
                reply["status"] = "error";
                reply["message"] = "Ação desconhecida";
            }
        } catch (const std::exception& error) {
            reply["status"] = "error";
            reply["message"] = "Requisição inválida";
            log_error("[", get_timestamp(), "][ERRO] Pedido de servidor inválido: ", error.what());
        }

        if (reply["timestamp"].empty()) {
            reply["timestamp"] = get_timestamp();
        }
        const long long clock = tick();
        log_line("[", get_timestamp(), "][ENVIADO SERVIDOR] ", map_to_string(reply), " | logical_clock: ", clock);
        send_message(socket, reply, clock);
    }
}

std::string peer_endpoint(const std::string& name) {
    return "tcp://" + name + ":" + std::to_string(peer_port);
}

// Publica no tópico "servers" que este servidor é o coordenador.
void announce_coordinator(zmq::socket_t& pub_socket) {
    set_coordinator(server_name);
    std::map<std::string, std::string> announcement = {
        {"coordinator", server_name},
        {"rank", std::to_string(my_rank)},
        {"timestamp", get_timestamp()}};
    const long long clock = tick();
    msgpack::sbuffer buffer = encode_message(announcement, clock);
    const std::string topic = "servers";
    zmq::message_t topic_msg(topic.data(), topic.size());
    zmq::message_t content_msg(buffer.data(), buffer.size());
    pub_socket.send(topic_msg, zmq::send_flags::sndmore);
    pub_socket.send(content_msg, zmq::send_flags::none);
    log_line("[", announcement["timestamp"], "][COORDENADOR] Eleito; publicado em [servers] ",
             map_to_string(announcement), " | logical_clock: ", clock);
}

// Algoritmo do valentão (bully): pede eleição a todos os servidores de rank maior.
// Se nenhum responder OK, este servidor é o novo coordenador; se algum responder,
// espera o anúncio dele no tópico "servers".
// Devolve true se algum servidor de rank maior respondeu.
bool start_election(zmq::context_t& context, zmq::socket_t& pub_socket) {
    request_server_list(context);
    std::vector<std::pair<std::string, int>> higher;
    for (const auto& server : known_servers) {
        if (server.second > my_rank && server.first != server_name) {
            higher.push_back(server);
        }
    }
    log_line("[", get_timestamp(), "][ELEIÇÃO] Iniciando eleição (meu rank: ", my_rank, "); pedindo a ",
             higher.size(), " servidor(es) de rank maior");

    // Os pedidos são enviados a todos de uma vez e as respostas esperadas por até 1 s.
    std::vector<std::unique_ptr<zmq::socket_t>> sockets;
    std::vector<std::string> targets;  // nome do servidor de cada socket
    for (const auto& [name, rank] : higher) {
        auto socket = std::make_unique<zmq::socket_t>(context, zmq::socket_type::req);
        socket->set(zmq::sockopt::linger, 0);
        try {
            socket->connect(peer_endpoint(name));
        } catch (const zmq::error_t& error) {
            // Servidor que acabou de cair pode nem ter mais o nome na rede; conta como sem resposta.
            log_error("[", get_timestamp(), "][ELEIÇÃO] Não foi possível conectar a ", name, ": ", error.what());
            continue;
        }
        std::map<std::string, std::string> request = {
            {"action", "election"}, {"name", server_name}, {"rank", std::to_string(my_rank)},
            {"timestamp", get_timestamp()}};
        const long long clock = tick();
        log_line("[", request["timestamp"], "][REQ SERVIDOR] para ", name, ": ", map_to_string(request),
                 " | logical_clock: ", clock);
        send_message(*socket, request, clock);
        sockets.push_back(std::move(socket));
        targets.push_back(name);
    }

    bool someone_answered = false;
    std::vector<bool> answered(sockets.size(), false);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(1000);
    while (!sockets.empty() && std::chrono::steady_clock::now() < deadline) {
        std::vector<zmq::pollitem_t> items;
        for (const auto& socket : sockets) {
            items.push_back({socket->handle(), 0, ZMQ_POLLIN, 0});
        }
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        zmq::poll(items.data(), items.size(), std::max(remaining, std::chrono::milliseconds(0)));

        bool pending = false;
        for (std::size_t i = 0; i < sockets.size(); ++i) {
            if (answered[i]) continue;
            if (!(items[i].revents & ZMQ_POLLIN)) {
                pending = true;
                continue;
            }
            zmq::message_t reply;
            if (!sockets[i]->recv(reply, zmq::recv_flags::none)) continue;
            answered[i] = true;
            try {
                auto fields = decode_message(static_cast<const char*>(reply.data()), reply.size());
                update_clock(fields);
                log_line("[", get_timestamp(), "][REP SERVIDOR] de ", targets[i], ": ", map_to_string(fields));
                if (fields["status"] == "success") someone_answered = true;
            } catch (const std::exception&) {
            }
        }
        if (!pending) break;
    }

    if (someone_answered) {
        log_line("[", get_timestamp(), "][ELEIÇÃO] Servidor de rank maior respondeu; aguardando o anúncio do coordenador");
    } else {
        announce_coordinator(pub_socket);
    }
    return someone_answered;
}

// Sincronização com o coordenador a cada 15 mensagens: "REQ: relógio" -> "REP: hora correta".
// O servidor envia a sua hora; o coordenador responde a hora correta (Berkeley) e o
// servidor acerta o relógio somando metade do tempo de ida e volta.
void synchronize_with_coordinator(zmq::context_t& context) {
    const std::string current = get_coordinator();
    if (current.empty() || current == server_name) {
        return;
    }

    zmq::socket_t socket(context, zmq::socket_type::req);
    socket.set(zmq::sockopt::linger, 0);
    socket.set(zmq::sockopt::rcvtimeo, 1000);
    try {
        socket.connect(peer_endpoint(current));
    } catch (const zmq::error_t& error) {
        log_line("[", get_timestamp(), "][ELEIÇÃO] Coordenador ", current, " inacessível (", error.what(),
                 "); pedindo eleição");
        set_coordinator("");
        election_requested = true;
        return;
    }

    const long long sent_at = now_ms();
    std::map<std::string, std::string> request = {
        {"action", "clock"}, {"name", server_name}, {"rank", std::to_string(my_rank)},
        {"time", std::to_string(sent_at)}, {"timestamp", iso_timestamp(sent_at)}};
    const long long clock = tick();
    log_line("[", request["timestamp"], "][REQ SERVIDOR] para ", current, ": ", map_to_string(request),
             " | logical_clock: ", clock);
    send_message(socket, request, clock);

    zmq::message_t reply;
    if (!socket.recv(reply, zmq::recv_flags::none)) {
        log_line("[", get_timestamp(), "][ELEIÇÃO] Coordenador ", current, " não respondeu; pedindo eleição");
        set_coordinator("");
        election_requested = true;
        return;
    }
    const long long received_at = now_ms();

    try {
        auto fields = decode_message(static_cast<const char*>(reply.data()), reply.size());
        update_clock(fields);
        log_line("[", get_timestamp(), "][REP SERVIDOR] de ", current, ": ", map_to_string(fields));
        if (fields["status"] != "success") {
            set_coordinator("");
            election_requested = true;
            return;
        }
        long long coordinator_time = 0;
        if (!message_time_ms(fields, coordinator_time)) {
            log_error("[", get_timestamp(), "][RELÓGIO ERRO] Resposta do coordenador sem hora");
            return;
        }
        const long long estimated_time = coordinator_time + (received_at - sent_at) / 2;
        const long long adjustment = estimated_time - received_at;
        clock_offset_ms += adjustment;
        log_line("[", get_timestamp(), "][RELÓGIO] Ajuste de ", adjustment, " ms pelo coordenador ", current,
                 " (diferença atual para o sistema: ", clock_offset_ms.load(), " ms)");
    } catch (const std::exception& error) {
        log_error("[", get_timestamp(), "][RELÓGIO ERRO] Resposta inválida do coordenador: ", error.what());
    }
}

// Sincronização inicial (parte 5): ao subir, o servidor pede todos os dados a outro
// servidor disponível, para recuperar o que foi gravado enquanto ele estava fora.
// Tenta os servidores da lista da referência até um responder.
void initial_sync(zmq::context_t& context) {
    request_server_list(context);
    for (const auto& [name, rank] : known_servers) {
        if (name == server_name) continue;

        zmq::socket_t socket(context, zmq::socket_type::req);
        socket.set(zmq::sockopt::linger, 0);
        socket.set(zmq::sockopt::rcvtimeo, 2000);
        try {
            socket.connect(peer_endpoint(name));
        } catch (const zmq::error_t&) {
            continue;
        }
        std::map<std::string, std::string> request = {
            {"action", "sync"}, {"name", server_name}, {"rank", std::to_string(my_rank)},
            {"timestamp", get_timestamp()}};
        const long long clock = tick();
        log_line("[", request["timestamp"], "][REQ SERVIDOR] para ", name, ": ", map_to_string(request),
                 " | logical_clock: ", clock);
        send_message(socket, request, clock);

        zmq::message_t reply;
        if (!socket.recv(reply, zmq::recv_flags::none)) {
            log_line("[", get_timestamp(), "][REPLICAÇÃO] ", name, " não respondeu à sincronização");
            continue;
        }
        try {
            auto fields = decode_message(static_cast<const char*>(reply.data()), reply.size());
            update_clock(fields);
            if (fields["status"] != "success") continue;
            const int users = merge_lines(USERS_FILE, fields["users"]);
            const int channels = merge_lines(CHANNELS_FILE, fields["channels"]);
            const int messages = merge_lines(MESSAGES_FILE, fields["messages"]);
            log_line("[", get_timestamp(), "][REPLICAÇÃO] Sincronização inicial com ", name, ": ", users,
                     " login(s), ", channels, " canal(is) e ", messages, " mensagem(ns) novos");
            return;
        } catch (const std::exception& error) {
            log_error("[", get_timestamp(), "][REPLICAÇÃO] Resposta inválida de ", name, ": ", error.what());
        }
    }
    log_line("[", get_timestamp(), "][REPLICAÇÃO] Nenhum servidor respondeu à sincronização; seguindo com os dados locais");
}

// ---------------------------------------------------------------------------
// Requisições dos clientes
// ---------------------------------------------------------------------------

std::map<std::string, std::string> handle_request(std::map<std::string, std::string>& req_map,
                                                  zmq::socket_t& pub_socket) {
    std::string action = req_map["action"];
    std::string request_timestamp = req_map["timestamp"];
    std::map<std::string, std::string> rep_map;

    if (request_timestamp.empty()) {
        rep_map["status"] = "error";
        rep_map["message"] = "Timestamp ausente";
        log_line("[", get_timestamp(), "][ERRO] Timestamp ausente");
    }
    else if (action == "login") {
        std::string username = trim(req_map["username"]);
        if (username.empty()) {
            rep_map["status"] = "error";
            rep_map["message"] = "Nome de usuário inválido";
            log_line("[", request_timestamp, "][LOGIN ERRO] Nome inválido");
        } else {
            save_user(username, request_timestamp);
            log_line("[", request_timestamp, "][LOGIN] Usuário: ", username);
            rep_map["status"] = "success";
            rep_map["message"] = "Login realizado com sucesso";
            publish_replication(pub_socket, {{"type", "login"}, {"username", username},
                                             {"login_timestamp", request_timestamp}});
        }
    }
    else if (action == "create_channel") {
        std::string channel = trim(req_map["channel"]);
        auto channels = load_channels();
        if (channel.empty()) {
            rep_map["status"] = "error";
            rep_map["message"] = "Nome de canal inválido";
            log_line("[", request_timestamp, "][CANAL ERRO] Nome inválido");
        } else if (channels.find(channel) == channels.end()) {
            save_channel(channel);
            log_line("[", request_timestamp, "][CHANNEL] Canal criado: ", channel);
            rep_map["status"] = "success";
            rep_map["message"] = "Canal criado com sucesso";
            publish_replication(pub_socket, {{"type", "channel"}, {"channel", channel}});
        } else {
            rep_map["status"] = "error";
            rep_map["message"] = "Canal já existe";
            log_line("[", request_timestamp, "][CANAL ERRO] Já existe: ", channel);
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
        log_line("[", request_timestamp, "][CANAIS] Lista enviada: ", channel_list);
    }
    else if (action == "publish") {
        std::string channel = trim(req_map["channel"]);
        std::string message = req_map["message"];
        std::string username = req_map["username"];

        auto channels = load_channels();
        if (channel.empty() || trim(message).empty()) {
            rep_map["status"] = "error";
            rep_map["message"] = "Canal ou mensagem inválidos";
            log_line("[", request_timestamp, "][PUB ERRO] Canal ou mensagem inválidos");
        } else {
            // Com o broker, o canal pode ter sido criado em outro servidor;
            // ele passa a ser registrado também neste.
            if (channels.find(channel) == channels.end()) {
                save_channel(channel);
                log_line("[", request_timestamp, "][CHANNEL] Canal registrado a partir de publicação: ", channel);
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
            log_line("[", publication_timestamp, "][PUB] ", map_to_string(publication), " | logical_clock: ", clock);
            publish_replication(pub_socket, {{"type", "publication"}, {"channel", channel},
                                             {"username", username}, {"message", message},
                                             {"request_timestamp", request_timestamp},
                                             {"publication_timestamp", publication_timestamp}});
        }
    }
    else {
        rep_map["status"] = "error";
        rep_map["message"] = "Ação desconhecida";
        log_line("[", request_timestamp, "][ERRO] Ação desconhecida: ", action);
    }

    return rep_map;
}

// Anúncio recebido no tópico "servers": guarda o nome do coordenador.
void handle_announcement(const std::map<std::string, std::string>& fields) {
    const auto name = fields.find("coordinator");
    if (name == fields.end() || name->second.empty()) return;
    set_coordinator(name->second);
    log_line("[", get_timestamp(), "][COORDENADOR] Novo coordenador: ", name->second, " ", map_to_string(fields));

    // No algoritmo do valentão, um servidor de rank maior assume no lugar de um menor.
    const auto rank = fields.find("rank");
    if (name->second != server_name && rank != fields.end()) {
        try {
            if (std::stoi(rank->second) < my_rank) {
                election_requested = true;
            }
        } catch (const std::exception&) {
        }
    }
}

int main() {
    std::filesystem::create_directories("data");
    load_stored_lines();

    char hostname[256] = {};
    gethostname(hostname, sizeof(hostname) - 1);
    const char* configured_name = std::getenv("SERVER_NAME");
    server_name = configured_name ? configured_name : hostname;
    // Sem REFERENCE_ENDPOINT o servidor funciona sem o serviço de referência (e sem eleição).
    const char* configured_reference = std::getenv("REFERENCE_ENDPOINT");
    reference_endpoint = configured_reference ? configured_reference : "";
    const char* configured_offset = std::getenv("CLOCK_OFFSET_MS");
    if (configured_offset) {
        clock_offset_ms = std::atoll(configured_offset);
    }
    const char* configured_peer_port = std::getenv("PEER_PORT");
    if (configured_peer_port) {
        peer_port = std::atoi(configured_peer_port);
    }

    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::rep);
    zmq::socket_t pub_socket(context, zmq::socket_type::pub);
    zmq::socket_t sub_socket(context, zmq::socket_type::sub);

    // Com BROKER_BACKEND definido, o servidor conecta no DEALER do broker;
    // sem ele, atende os clientes diretamente na porta 5555.
    const char* broker_backend = std::getenv("BROKER_BACKEND");
    if (broker_backend) {
        socket.connect(broker_backend);
        log_line("[Servidor C++] ", server_name, " conectado ao broker em ", broker_backend);
    } else {
        socket.bind("tcp://*:5555");
        log_line("[Servidor C++] ", server_name, " iniciado na porta 5555...");
    }

    const char* pubsub_xsub = std::getenv("PUBSUB_XSUB_ENDPOINT");
    pub_socket.connect(pubsub_xsub ? pubsub_xsub : "tcp://proxy-pubsub:5557");
    // O servidor também assina o tópico "servers" para saber quem é o coordenador.
    const char* pubsub_xpub = std::getenv("PUBSUB_XPUB_ENDPOINT");
    sub_socket.connect(pubsub_xpub ? pubsub_xpub : "tcp://proxy-pubsub:5558");
    sub_socket.set(zmq::sockopt::subscribe, "servers");
    // E o tópico "replication", para receber os dados gravados pelos outros servidores.
    sub_socket.set(zmq::sockopt::subscribe, REPLICATION_TOPIC);
    replication_prefix = server_name + ":" + std::to_string(now_ms());
    log_line("[Servidor C++] Publisher conectado ao proxy Pub/Sub e inscrito nos tópicos servers e replication");
    if (clock_offset_ms != 0) {
        log_line("[", get_timestamp(), "][RELÓGIO] Iniciando com atraso simulado de ", clock_offset_ms.load(), " ms");
    }

    const bool use_reference = !reference_endpoint.empty();
    if (use_reference) {
        connect_reference(context);
        request_rank(context);
        std::thread(peer_loop, std::ref(context)).detach();
        // Dá tempo de a inscrição nos tópicos chegar ao proxy antes da eleição e da sincronização.
        sleep(1);
        initial_sync(context);
        election_requested = true;
    }

    auto waiting_announcement_until = std::chrono::steady_clock::time_point::min();
    auto last_election = std::chrono::steady_clock::now() - std::chrono::seconds(10);

    while (true) {
        zmq::pollitem_t items[] = {{socket.handle(), 0, ZMQ_POLLIN, 0}, {sub_socket.handle(), 0, ZMQ_POLLIN, 0}};
        zmq::poll(items, 2, std::chrono::milliseconds(500));

        // Anúncios de coordenador no tópico "servers".
        if (items[1].revents & ZMQ_POLLIN) {
            zmq::message_t topic_frame;
            zmq::message_t content_frame;
            if (sub_socket.recv(topic_frame, zmq::recv_flags::none) && topic_frame.more() &&
                sub_socket.recv(content_frame, zmq::recv_flags::none)) {
                const std::string topic(static_cast<const char*>(topic_frame.data()), topic_frame.size());
                if (topic == "servers") {
                    try {
                        auto fields = decode_message(static_cast<const char*>(content_frame.data()), content_frame.size());
                        update_clock(fields);
                        handle_announcement(fields);
                        waiting_announcement_until = std::chrono::steady_clock::time_point::min();
                    } catch (const std::exception& error) {
                        log_error("[", get_timestamp(), "][ERRO] Anúncio inválido: ", error.what());
                    }
                } else if (topic == REPLICATION_TOPIC) {
                    try {
                        auto fields = decode_message(static_cast<const char*>(content_frame.data()), content_frame.size());
                        update_clock(fields);
                        apply_replication(fields);
                    } catch (const std::exception& error) {
                        log_error("[", get_timestamp(), "][ERRO] Replicação inválida: ", error.what());
                    }
                }
            }
        }

        // Requisições dos clientes.
        if (items[0].revents & ZMQ_POLLIN) {
            zmq::message_t request;
            if (socket.recv(request, zmq::recv_flags::none)) {
                std::map<std::string, std::string> rep_map;
                try {
                    std::map<std::string, std::string> req_map =
                        decode_message(static_cast<const char*>(request.data()), request.size());
                    update_clock(req_map);
                    log_line("[", get_timestamp(), "][RECEBIDO] ", map_to_string(req_map));
                    rep_map = handle_request(req_map, pub_socket);
                } catch (const std::exception& error) {
                    rep_map["status"] = "error";
                    rep_map["message"] = "Requisição inválida";
                    log_error("[", get_timestamp(), "][ERRO] Requisição inválida: ", error.what());
                }

                rep_map["timestamp"] = get_timestamp();
                const long long clock = tick();
                log_line("[", rep_map["timestamp"], "][ENVIADO] ", map_to_string(rep_map), " | logical_clock: ", clock);
                send_message(socket, rep_map, clock);

                client_messages++;
                if (use_reference) {
                    // A cada 10 mensagens de clientes, avisa a referência que está ativo.
                    if (client_messages % HEARTBEAT_INTERVAL == 0) {
                        send_heartbeat(context);
                    }
                    // A cada 15 mensagens, sincroniza o relógio com o coordenador.
                    if (client_messages % CLOCK_SYNC_INTERVAL == 0) {
                        synchronize_with_coordinator(context);
                    }
                }
            }
        }

        if (!use_reference) {
            continue;
        }

        // Se um servidor de rank maior respondeu mas o anúncio não chegou, tenta de novo.
        const auto now = std::chrono::steady_clock::now();
        if (waiting_announcement_until != std::chrono::steady_clock::time_point::min() &&
            now > waiting_announcement_until) {
            log_line("[", get_timestamp(), "][ELEIÇÃO] Anúncio do coordenador não chegou; repetindo a eleição");
            waiting_announcement_until = std::chrono::steady_clock::time_point::min();
            election_requested = true;
        }

        // Eleição pedida por outro servidor, por falha do coordenador ou por não haver coordenador.
        const bool no_coordinator = get_coordinator().empty() &&
                                    waiting_announcement_until == std::chrono::steady_clock::time_point::min();
        if ((election_requested || no_coordinator) && now - last_election > std::chrono::seconds(3)) {
            election_requested = false;
            last_election = now;
            if (start_election(context, pub_socket)) {
                waiting_announcement_until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            }
        }
    }

    return 0;
}
