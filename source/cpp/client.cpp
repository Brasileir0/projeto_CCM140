#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <ctime>
#include <cstdlib>
#include <thread>
#include <zmq.hpp>
#include <msgpack.hpp>
#include <map>
#include <thread>
#include <set>
#include <vector>
#include <random>

std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm local_time{};
    localtime_r(&in_time_t, &local_time);
    std::stringstream ss;
    ss << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");
    return ss.str();
}

void send_request(
    zmq::socket_t& socket,
    std::map<std::string, std::string> request_data,
    std::map<std::string, std::string>& response_data,
    const std::string& response_field) {
    const std::string timestamp = get_timestamp();
    request_data["timestamp"] = timestamp;
    std::cout << "[" << timestamp << "][REQ] " << request_data["action"] << std::endl;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, request_data);
    zmq::message_t request(sbuf.data(), sbuf.size());
    socket.send(request, zmq::send_flags::none);

    zmq::message_t reply;
    auto receive_result = socket.recv(reply, zmq::recv_flags::none);

    msgpack::object_handle oh = msgpack::unpack(static_cast<const char*>(reply.data()), reply.size());
    oh.get().convert(response_data);
    std::cout << "[" << timestamp << "][REP] " << response_data[response_field] << std::endl;
}

void subscriber_loop(const std::string& pub_endpoint, std::set<std::string>& subscribed_channels) {
    zmq::context_t sub_context(1);
    zmq::socket_t sub_socket(sub_context, zmq::socket_type::sub);
    sub_socket.connect(pub_endpoint);

    // Inscreve-se nos canais desejados dinamicamente
    for (const auto& ch : subscribed_channels) {
        sub_socket.set(zmq::sockopt::subscribe, ch);
        std::cout << "[Cliente SUB] Inscrito no topico/canal: " << ch << std::endl;
    }

    while (true) {
        zmq::message_t channel_frame;
        zmq::message_t payload_frame;
        
        auto res1 = sub_socket.recv(channel_frame, zmq::recv_flags::none);
        auto res2 = sub_socket.recv(payload_frame, zmq::recv_flags::none);

        if (res1 && res2) {
            std::string channel(static_cast<char*>(channel_frame.data()), channel_frame.size());
            std::string content(static_cast<char*>(payload_frame.data()), payload_frame.size());
            std::string receive_time = get_timestamp();

            std::cout << "\n[RECEBIDO PUB] Canal: [" << channel << "] | Conteudo: " << content 
                      << " | Hora Recebimento: " << receive_time << std::endl;
        }
    }
}

int main() {
    zmq::context_t context(1);
    const char* configured_endpoints = std::getenv("SERVER_ENDPOINTS");
    std::stringstream endpoint_stream(configured_endpoints ? configured_endpoints : "tcp://server-cpp:5555");
    std::string endpoint;

    while (std::getline(endpoint_stream, endpoint, ',')) {
        if (endpoint.empty()) continue;
        std::cout << "[Cliente C++] Conectando a " << endpoint << std::endl;
        zmq::socket_t socket(context, zmq::socket_type::req);
        socket.connect(endpoint);

        std::map<std::string, std::string> login_request = {
            {"action", "login"},
            {"username", "CPP"}
        };
        std::map<std::string, std::string> login_response;
        send_request(socket, login_request, login_response, "message");
        while (login_response["status"] != "success") {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            send_request(socket, login_request, login_response, "message");
        }

        std::map<std::string, std::string> create_request = {
            {"action", "create_channel"},
            {"channel", "geral"}
        };
        std::map<std::string, std::string> create_response;
        send_request(socket, create_request, create_response, "message");

        std::map<std::string, std::string> list_request = {
            {"action", "list_channels"}
        };
        std::map<std::string, std::string> list_response;
        send_request(socket, list_request, list_response, "channels");
        socket.close();
    }

    return 0;
}

// std::set<std::string> my_subscriptions = {"geral", "tecnologia", "geral2"}; // Exemplo
// std::thread sub_thread(subscriber_loop, "tcp://proxy-pubsub:5558", std::ref(my_subscriptions));
// sub_thread.detach();

// proxy-pubsub:
//     build:
//       context: .
//       dockerfile: Dockerfile.proxy # ou use um Dockerfile dedicado ao broker
//     ports:
//       - "5557:5557"
//       - "5558:5558"
//     networks:
//       - rede-projeto