#include <iostream>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <zmq.hpp>
#include <msgpack.hpp>
#include <map>

std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");
    return ss.str();
}

void send_request(zmq::socket_t& socket, const std::map<std::string, std::string>& request_data, std::map<std::string, std::string>& response_data) {
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, request_data);
    zmq::message_t request(sbuf.data(), sbuf.size());
    socket.send(request, zmq::send_flags::none);

    zmq::message_t reply;
    socket.recv(reply, zmq::recv_flags::none);

    msgpack::object_handle oh = msgpack::unpack(static_cast<const char*>(reply.data(), reply.size()));
    oh.get().convert(response_data);
}

int main() {
    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::req);
    socket.connect("tcp://server-cpp:5555");

    std::string timestamp = get_timestamp();

    // LOGIN
    std::map<std::string, std::string> login_req = {
        {"action", "login"},
        {"username", "CPP"},
        {"timestamp", timestamp}
    };
    std::map<std::string, std::string> login_rep;
    send_request(socket, login_req, login_rep);
    std::cout << "[" << timestamp << "][LOGIN RESP]" << login_rep["message"] << "\n";

    // CRIAR CANAL
    std::map<std::string, std::string> create_req = {
        {"action", "create_channel"},
        {"channel", "geral"},
        {"timestamp", timestamp}
    };
    std::map<std::string, std::string> create_rep;
    send_request(socket, create_req, create_rep);
    std::cout << "[" << timestamp << "][CHANNEL RESP] " << create_rep["message"] << "\n";

    // LISTAR CANAIS
    std::map<std::string, std::string> list_req = {
        {"action", "list_channels"},
        {"timestamp", timestamp}
    };
    std::map<std::string, std::string> list_rep;
    send_request(socket, list_req, list_rep);
    std::cout << "[" << timestamp << "][CANAIS]" << list_rep["channels"] << "\n";

    return 0;
}