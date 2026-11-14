#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <set>
#include <ctime>
#include <filesystem>
#include <zmq.hpp>
#include <msgpack.hpp>
#include <map>

std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm local_time{};
    localtime_r(&in_time_t, &local_time);
    std::stringstream ss;
    ss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");
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
            msgpack::sbuffer publication_buffer;
            msgpack::pack(publication_buffer, publication);
            zmq::message_t topic_msg(channel.data(), channel.size());
            zmq::message_t content_msg(publication_buffer.data(), publication_buffer.size());

            pub_socket.send(topic_msg, zmq::send_flags::sndmore);
            pub_socket.send(content_msg, zmq::send_flags::none);

            rep_map["status"] = "success";
            rep_map["message"] = "Mensagem publicada com sucesso";
            std::cout << "[" << publication_timestamp << "][PUB] " << map_to_string(publication) << std::endl;
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
    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::rep);
    zmq::socket_t pub_socket(context, zmq::socket_type::pub);

    // Com BROKER_BACKEND definido, o servidor conecta no DEALER do broker;
    // sem ele, atende os clientes diretamente na porta 5555.
    const char* broker_backend = std::getenv("BROKER_BACKEND");
    if (broker_backend) {
        socket.connect(broker_backend);
        std::cout << "[Servidor C++] Conectado ao broker em " << broker_backend << std::endl;
    } else {
        socket.bind("tcp://*:5555");
        std::cout << "[Servidor C++] Iniciado na porta 5555..." << std::endl;
    }

    const char* pubsub_xsub = std::getenv("PUBSUB_XSUB_ENDPOINT");
    pub_socket.connect(pubsub_xsub ? pubsub_xsub : "tcp://proxy-pubsub:5557");
    std::cout << "[Servidor C++] Publisher conectado ao proxy Pub/Sub" << std::endl;

    while (true) {
        zmq::message_t request;
        if (!socket.recv(request, zmq::recv_flags::none)) {
            continue;
        }

        std::map<std::string, std::string> rep_map;
        try {
            msgpack::object_handle oh = msgpack::unpack(static_cast<const char*>(request.data()), request.size());
            std::map<std::string, std::string> req_map;
            oh.get().convert(req_map);
            std::cout << "[" << get_timestamp() << "][RECEBIDO] " << map_to_string(req_map) << std::endl;
            rep_map = handle_request(req_map, pub_socket);
        } catch (const std::exception& error) {
            rep_map["status"] = "error";
            rep_map["message"] = "Requisição inválida";
            std::cerr << "[" << get_timestamp() << "][ERRO] Requisição inválida: " << error.what() << std::endl;
        }

        rep_map["timestamp"] = get_timestamp();
        std::cout << "[" << rep_map["timestamp"] << "][ENVIADO] " << map_to_string(rep_map) << std::endl;

        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, rep_map);
        zmq::message_t reply(sbuf.data(), sbuf.size());
        socket.send(reply, zmq::send_flags::none);
    }

    return 0;
}
