#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include <zmq.hpp>
#include <msgpack.hpp>

std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm local_time{};
    localtime_r(&in_time_t, &local_time);
    std::stringstream stream;
    stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");
    return stream.str();
}

// Normaliza os timestamps dos servidores (ISO do Python/JS ou "data hora" do C++)
// para "AAAA-MM-DD HH:MM:SS".
std::string format_timestamp(const std::string& timestamp) {
    if (timestamp.size() >= 19 &&
        (timestamp[10] == 'T' || timestamp[10] == ' ')) {
        return timestamp.substr(0, 10) + " " + timestamp.substr(11, 8);
    }
    return timestamp;
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

// Fila de canais que a thread principal pede para a thread do SUB assinar.
// Sockets ZeroMQ não são thread-safe, então só a thread do SUB mexe no socket.
std::mutex subscription_mutex;
std::queue<std::string> pending_subscriptions;

std::map<std::string, std::string> send_request(
    zmq::socket_t& socket,
    std::map<std::string, std::string> request) {
    request["timestamp"] = get_timestamp();
    std::cout << "[" << request["timestamp"] << "][REQ] "
              << map_to_string(request) << std::endl;

    msgpack::sbuffer buffer;
    msgpack::pack(buffer, request);
    zmq::message_t message(buffer.data(), buffer.size());
    if (!socket.send(message, zmq::send_flags::none)) {
        throw std::runtime_error("Falha ao enviar requisição ao servidor.");
    }

    zmq::message_t reply;
    if (!socket.recv(reply, zmq::recv_flags::none)) {
        throw std::runtime_error("Falha ao receber resposta do servidor.");
    }

    msgpack::object_handle handle =
        msgpack::unpack(static_cast<const char*>(reply.data()), reply.size());
    std::map<std::string, std::string> response;
    handle.get().convert(response);
    const auto response_timestamp = response.find("timestamp");
    const std::string displayed_timestamp =
        response_timestamp == response.end() ? get_timestamp()
                                             : format_timestamp(response_timestamp->second);
    std::cout << "[" << displayed_timestamp << "][REP] "
              << map_to_string(response) << std::endl;
    return response;
}

bool is_success(const std::map<std::string, std::string>& response) {
    const auto status = response.find("status");
    return status != response.end() && status->second == "success";
}

std::string response_message(const std::map<std::string, std::string>& response) {
    const auto message = response.find("message");
    return message == response.end() ? "resposta inválida" : message->second;
}

std::vector<std::string> parse_channels(const std::string& value) {
    std::vector<std::string> channels;
    std::stringstream stream(value);
    std::string channel;
    while (std::getline(stream, channel, ';')) {
        if (!channel.empty()) {
            channels.push_back(channel);
        }
    }
    return channels;
}

std::vector<std::string> list_channels(zmq::socket_t& socket) {
    const auto response = send_request(socket, {{"action", "list_channels"}});
    if (!is_success(response)) {
        throw std::runtime_error(
            "Não foi possível listar canais: " + response_message(response));
    }
    const auto channels = response.find("channels");
    if (channels == response.end()) {
        throw std::runtime_error("A resposta do servidor não contém canais.");
    }
    return parse_channels(channels->second);
}

void subscriber_loop(const std::string& endpoint) {
    zmq::context_t context(1);
    zmq::socket_t subscriber(context, zmq::socket_type::sub);
    subscriber.connect(endpoint);
    std::set<std::string> subscribed_channels;

    while (true) {
        {
            std::lock_guard<std::mutex> lock(subscription_mutex);
            while (!pending_subscriptions.empty()) {
                const std::string channel = pending_subscriptions.front();
                pending_subscriptions.pop();
                subscriber.set(zmq::sockopt::subscribe, channel);
                subscribed_channels.insert(channel);
                std::cout << "[" << get_timestamp() << "][SUB] Inscrito no canal: "
                          << channel << std::endl;
            }
        }

        // Timeout curto para conseguir aplicar novas inscrições da fila.
        zmq::pollitem_t items[] = {{subscriber.handle(), 0, ZMQ_POLLIN, 0}};
        zmq::poll(items, 1, std::chrono::milliseconds(200));
        if (!(items[0].revents & ZMQ_POLLIN)) {
            continue;
        }

        zmq::message_t topic_frame;
        zmq::message_t publication_frame;
        if (!subscriber.recv(topic_frame, zmq::recv_flags::none)) {
            continue;
        }
        if (!topic_frame.more() ||
            !subscriber.recv(publication_frame, zmq::recv_flags::none)) {
            std::cerr << "[SUB][ERRO] Formato de publicação inválido" << std::endl;
            continue;
        }

        const std::string channel(
            static_cast<const char*>(topic_frame.data()), topic_frame.size());
        // O filtro do ZeroMQ é por prefixo ("canal-a" também casa com "canal-ab").
        if (subscribed_channels.find(channel) == subscribed_channels.end()) {
            continue;
        }

        try {
            msgpack::object_handle handle = msgpack::unpack(
                static_cast<const char*>(publication_frame.data()),
                publication_frame.size());
            std::map<std::string, std::string> publication;
            handle.get().convert(publication);
            std::cout << "\n-------- NOVA MENSAGEM ----------\n"
                      << "Canal: " << channel << "\n";
            if (publication.count("username")) {
                std::cout << "Autor: " << publication["username"] << "\n";
            }
            std::cout << "Mensagem: " << publication["message"] << "\n"
                      << "Enviada em: "
                      << format_timestamp(publication["timestamp"]) << "\n"
                      << "Recebida em: " << get_timestamp() << "\n"
                      << "-----------------------------------\n"
                      << std::endl;
        } catch (const std::exception& error) {
            std::cerr << "[SUB][ERRO] Publicação inválida: "
                      << error.what() << std::endl;
        }
    }
}

int main() {
    try {
        const char* configured_endpoints = std::getenv("SERVER_ENDPOINTS");
        std::stringstream endpoint_stream(
            configured_endpoints ? configured_endpoints : "tcp://server-cpp:5555");
        std::vector<std::string> endpoints;
        std::string endpoint;
        while (std::getline(endpoint_stream, endpoint, ',')) {
            if (!endpoint.empty()) {
                endpoints.push_back(endpoint);
            }
        }
        if (endpoints.empty()) {
            throw std::runtime_error("SERVER_ENDPOINTS não contém endpoints válidos.");
        }

        std::random_device random_device;
        std::mt19937 generator(random_device());
        std::uniform_int_distribution<std::size_t> endpoint_choice(0, endpoints.size() - 1);
        endpoint = endpoints[endpoint_choice(generator)];
        const char* configured_name = std::getenv("BOT_NAME");
        const std::string username =
            configured_name ? configured_name : "CPP-" + std::to_string(getpid());

        zmq::context_t context(1);
        zmq::socket_t request_socket(context, zmq::socket_type::req);
        request_socket.connect(endpoint);
        std::cout << "[Cliente C++] Conectando a " << endpoint
                  << " como " << username << std::endl;

        // Em caso de erro, o login é repetido (requisito da parte 1).
        while (true) {
            const auto login = send_request(
                request_socket, {{"action", "login"}, {"username", username}});
            if (is_success(login)) break;
            std::cerr << "[Cliente C++] Login falhou: " << response_message(login)
                      << ". Tentando novamente..." << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        const char* configured_pubsub = std::getenv("PUBSUB_XPUB_ENDPOINT");
        const std::string pubsub_endpoint =
            configured_pubsub ? configured_pubsub : "tcp://proxy-pubsub:5558";
        std::thread(subscriber_loop, pubsub_endpoint).detach();

        std::set<std::string> subscriptions;
        std::uniform_int_distribution<int> character_choice(0, 35);
        const std::string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";

        while (true) {
            // 1. Se existirem menos do que 5 canais, cria um novo.
            auto channels = list_channels(request_socket);
            if (channels.size() < 5) {
                const std::string new_channel =
                    "canal-" + username + "-" +
                    std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count());
                const auto created = send_request(
                    request_socket,
                    {{"action", "create_channel"}, {"channel", new_channel}});
                if (is_success(created)) {
                    channels = list_channels(request_socket);
                } else {
                    std::cerr << "[Cliente C++] Não foi possível criar o canal "
                              << new_channel << ": " << response_message(created)
                              << std::endl;
                }
            }
            if (channels.empty()) {
                std::cerr << "[Cliente C++] Nenhum canal disponível, tentando novamente..."
                          << std::endl;
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

            // 2. Se estiver inscrito em menos do que 3 canais, inscreve-se em mais um.
            if (subscriptions.size() < 3) {
                std::vector<std::string> candidates;
                for (const auto& channel : channels) {
                    if (subscriptions.find(channel) == subscriptions.end()) {
                        candidates.push_back(channel);
                    }
                }
                if (!candidates.empty()) {
                    std::uniform_int_distribution<std::size_t> candidate_choice(
                        0, candidates.size() - 1);
                    const std::string channel = candidates[candidate_choice(generator)];
                    subscriptions.insert(channel);
                    std::lock_guard<std::mutex> lock(subscription_mutex);
                    pending_subscriptions.push(channel);
                }
            }

            // 3. Escolhe um canal e envia 10 mensagens com 1 segundo de intervalo.
            std::uniform_int_distribution<std::size_t> channel_choice(0, channels.size() - 1);
            const std::string channel = channels[channel_choice(generator)];
            for (int index = 0; index < 10; ++index) {
                std::string random_text(8, ' ');
                for (char& character : random_text) {
                    character = alphabet[character_choice(generator)];
                }
                const std::string message =
                    "Mensagem " + std::to_string(index + 1) + " de " +
                    username + ": " + random_text;
                const auto response = send_request(
                    request_socket,
                    {{"action", "publish"},
                     {"channel", channel},
                     {"message", message},
                     {"username", username}});
                if (!is_success(response)) {
                    std::cerr << "[ERRO PUBLICAÇÃO] " << response_message(response)
                              << std::endl;
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "[Cliente C++] Falha: " << error.what() << std::endl;
        return 1;
    }
}
