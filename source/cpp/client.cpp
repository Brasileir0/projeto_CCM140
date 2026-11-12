#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <map>
#include <random>
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
    stream << std::put_time(&local_time, "%Y-%m-%d %H:%M");
    return stream.str();
}

std::string format_timestamp(const std::string& timestamp) {
    if (timestamp.size() >= 16 &&
        (timestamp[10] == 'T' || timestamp[10] == ' ')) {
        return timestamp.substr(0, 10) + " " + timestamp.substr(11, 5);
    }
    return timestamp;
}

std::map<std::string, std::string> send_request(
    zmq::socket_t& socket,
    std::map<std::string, std::string> request) {
    const std::string timestamp = get_timestamp();
    request["timestamp"] = timestamp;
    std::cout << "[" << format_timestamp(timestamp) << "][REQ] "
              << request["action"] << std::endl;

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
    const auto message_it = response.find("message");
    const auto status_it = response.find("status");
    const auto response_timestamp = response.find("timestamp");
    const std::string displayed_timestamp =
        response_timestamp == response.end() ? timestamp : response_timestamp->second;
    std::cout << "[" << format_timestamp(displayed_timestamp) << "][REP] "
              << (message_it != response.end()
                      ? message_it->second
                      : status_it != response.end() ? status_it->second : "")
              << std::endl;
    return response;
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
    const auto status = response.find("status");
    if (status == response.end() || status->second != "success") {
        const auto message = response.find("message");
        throw std::runtime_error(
            "Não foi possível listar canais: " +
            (message == response.end() ? "resposta inválida" : message->second));
    }
    const auto channels = response.find("channels");
    if (channels == response.end()) {
        throw std::runtime_error("A resposta do servidor não contém canais.");
    }
    return parse_channels(channels->second);
}

void subscriber_loop(
    const std::string& endpoint,
    const std::vector<std::string>& subscribed_channels) {
    zmq::context_t context(1);
    zmq::socket_t subscriber(context, zmq::socket_type::sub);
    subscriber.connect(endpoint);
    for (const auto& channel : subscribed_channels) {
        subscriber.set(zmq::sockopt::subscribe, channel);
        std::cout << "[Cliente SUB] Inscrito no canal: " << channel << std::endl;
    }

    while (true) {
        zmq::message_t topic_frame;
        zmq::message_t publication_frame;
        if (!subscriber.recv(topic_frame, zmq::recv_flags::none) ||
            !subscriber.recv(publication_frame, zmq::recv_flags::none)) {
            continue;
        }

        const std::string channel(
            static_cast<const char*>(topic_frame.data()), topic_frame.size());
        try {
            msgpack::object_handle handle = msgpack::unpack(
                static_cast<const char*>(publication_frame.data()),
                publication_frame.size());
            std::map<std::string, std::string> publication;
            handle.get().convert(publication);
            std::cout << "\n-------- NOVA MENSAGEM ----------\n"
                      << "Canal: " << channel << "\n"
                      << "Mensagem: " << publication["message"] << "\n"
                      << "Enviada em: "
                      << format_timestamp(publication["timestamp"]) << "\n"
                      << "Recebida em: "
                      << format_timestamp(get_timestamp()) << "\n"
                      << "-----------------------------------\n"
                      << std::endl;
        } catch (const std::exception& error) {
            std::cerr << "[Cliente SUB] Publicação inválida: "
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

        const auto login = send_request(
            request_socket, {{"action", "login"}, {"username", username}});
        if (login.find("status") == login.end() || login.at("status") != "success") {
            throw std::runtime_error("Login falhou.");
        }

        auto channels = list_channels(request_socket);
        if (channels.size() < 5) {
            const std::string new_channel =
                "canal-" + username + "-" +
                std::to_string(std::chrono::system_clock::now()
                                   .time_since_epoch()
                                   .count());
            const auto created = send_request(
                request_socket,
                {{"action", "create_channel"}, {"channel", new_channel}});
            if (created.find("status") == created.end() ||
                created.at("status") != "success") {
                throw std::runtime_error("Não foi possível criar um canal.");
            }
            channels = list_channels(request_socket);
        }
        if (channels.empty()) {
            throw std::runtime_error("O servidor não retornou canais disponíveis.");
        }

        std::shuffle(channels.begin(), channels.end(), generator);
        std::vector<std::string> subscriptions(
            channels.begin(),
            channels.begin() + std::min<std::size_t>(3, channels.size()));
        const char* configured_pubsub = std::getenv("PUBSUB_XPUB_ENDPOINT");
        const std::string pubsub_endpoint =
            configured_pubsub ? configured_pubsub : "tcp://proxy-pubsub:5558";
        std::thread(
            subscriber_loop, pubsub_endpoint, std::move(subscriptions))
            .detach();
        std::this_thread::sleep_for(std::chrono::milliseconds(250));

        std::uniform_int_distribution<std::size_t> channel_choice(0, channels.size() - 1);
        std::uniform_int_distribution<int> character_choice(0, 35);
        const std::string alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
        while (true) {
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
                if (response.find("status") == response.end() ||
                    response.at("status") != "success") {
                    const auto error = response.find("message");
                    std::cerr << "[ERRO PUBLICAÇÃO] "
                              << (error == response.end() ? "falha" : error->second)
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
