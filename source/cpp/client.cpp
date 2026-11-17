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

std::mutex subscription_mutex;
std::queue<std::string> pending_subscriptions;

std::mutex output_mutex;

void log_output(std::ostream& stream, const std::string& text) {
    std::lock_guard<std::mutex> lock(output_mutex);
    stream << text << std::endl;
}

std::mutex clock_mutex;
long long logical_clock = 0;

long long tick() {
    std::lock_guard<std::mutex> lock(clock_mutex);
    return ++logical_clock;
}

long long update_clock(const std::map<std::string, std::string>& message) {
    std::lock_guard<std::mutex> lock(clock_mutex);
    const auto clock = message.find("logical_clock");
    if (clock != message.end()) {
        try {
            logical_clock = std::max(logical_clock, std::stoll(clock->second));
        } catch (const std::exception&) {
        }
    }
    return logical_clock;
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

std::map<std::string, std::string> send_request(
    zmq::socket_t& socket,
    std::map<std::string, std::string> request) {
    request["timestamp"] = get_timestamp();
    const long long clock = tick();
    log_output(std::cout, "[" + request["timestamp"] + "][REQ] " + map_to_string(request) +
                              " | logical_clock: " + std::to_string(clock));

    msgpack::sbuffer buffer = encode_message(request, clock);
    zmq::message_t message(buffer.data(), buffer.size());
    if (!socket.send(message, zmq::send_flags::none)) {
        throw std::runtime_error("Falha ao enviar requisição ao servidor.");
    }

    zmq::message_t reply;
    if (!socket.recv(reply, zmq::recv_flags::none)) {
        throw std::runtime_error("Falha ao receber resposta do servidor.");
    }

    std::map<std::string, std::string> response =
        decode_message(static_cast<const char*>(reply.data()), reply.size());
    update_clock(response);
    const auto response_timestamp = response.find("timestamp");
    const std::string displayed_timestamp =
        response_timestamp == response.end() ? get_timestamp()
                                             : format_timestamp(response_timestamp->second);
    log_output(std::cout, "[" + displayed_timestamp + "][REP] " + map_to_string(response));
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
                log_output(std::cout, "[" + get_timestamp() + "][SUB] Inscrito no canal: " + channel);
            }
        }

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
            log_output(std::cerr, "[SUB][ERRO] Formato de publicação inválido");
            continue;
        }

        const std::string channel(
            static_cast<const char*>(topic_frame.data()), topic_frame.size());
        if (subscribed_channels.find(channel) == subscribed_channels.end()) {
            continue;
        }

        try {
            std::map<std::string, std::string> publication = decode_message(
                static_cast<const char*>(publication_frame.data()),
                publication_frame.size());
            const long long local_clock = update_clock(publication);
            std::ostringstream block;
            block << "\n-------- NOVA MENSAGEM ----------\n"
                  << "Canal: " << channel << "\n";
            if (publication.count("username")) {
                block << "Autor: " << publication["username"] << "\n";
            }
            block << "Mensagem: " << publication["message"] << "\n"
                  << "Enviada em: "
                  << format_timestamp(publication["timestamp"]) << "\n"
                  << "Recebida em: " << get_timestamp() << "\n"
                  << "Relógio lógico: "
                  << (publication.count("logical_clock") ? publication["logical_clock"] : "indisponível")
                  << " (local após receber: " << local_clock << ")\n"
                  << "-----------------------------------\n";
            log_output(std::cout, block.str());
        } catch (const std::exception& error) {
            log_output(std::cerr, std::string("[SUB][ERRO] Publicação inválida: ") + error.what());
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
                    log_output(std::cerr, "[Cliente C++] Não foi possível criar o canal " +
                                              new_channel + ": " + response_message(created));
                }
            }
            if (channels.empty()) {
                log_output(std::cerr, "[Cliente C++] Nenhum canal disponível, tentando novamente...");
                std::this_thread::sleep_for(std::chrono::seconds(1));
                continue;
            }

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
                    log_output(std::cerr, "[ERRO PUBLICAÇÃO] " + response_message(response));
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    } catch (const std::exception& error) {
        std::cerr << "[Cliente C++] Falha: " << error.what() << std::endl;
        return 1;
    }
}
