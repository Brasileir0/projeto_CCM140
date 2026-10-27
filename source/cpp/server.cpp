#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <iomanip>
#include <set>
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

void save_user(const std::string& username, const std::string& timestamp) {
    std::ofstream file("users.txt", std::ios::app);
    file << username << "," << timestamp << "\n";
}

void save_channel(const std::string& channel) {
    std::ofstream file ("channels.txt", std::ios::app);
    file << channel << "\n";
}

std::set<std::string> load_channels() {
    std::set<std::string> channels;
    std::ifstream file ("channels.txt");
    std::string line;
    while (std::getline(file, line)) {
        if (!line.empty()) channels.insert(line);
    }
    return channels;
}

int main() {
    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::rep);
    socket.bind("tcp://*:5555");
    std::cout << "[Servidor C++] Iniciado na porta 5555... \n";

    while (true) {
        zmq::message_t request;
        auto res = socket.recv(request, zmq::recv_flags::none);

        msgpack::object_handle oh = msgpack::unpack(static_cast<const char*>(request.data()), request.size());
        msgpack::object obj = oh.get();

        std::map<std::string, std::string> req_map;
        obj.convert(req_map);

        std::string action = req_map["action"];
        std::string timestamp = get_timestamp();
        std::map<std::string, std::string> rep_map;

        if (action == "login") {
            std::string username = req_map["username"];
            save_user(username, timestamp);
            std::cout << "[" << timestamp << "][LOGIN] Usuário: " << username << "\n";
            rep_map["status"] = "success";
            rep_map["message"] = "Login realizado com sucesso";
        }
        else if (action == "create_channel") {
            std::string channel = req_map["channel"];
            auto channels = load_channels();
            if (channels.find(channel) == channels.end()) {
                save_channel(channel);
                std::cout << "[" << timestamp << "][CHANNEL] Canal criado: " << channel << "\n"; 
                rep_map["status"] = "success";
                rep_map["message"] = "Canal criado com sucesso";
            } else {
                rep_map["status"] = "error";
                rep_map["message"] = "Canal já existe";
            }
        }
        else if (action == "list_channels") {
            auto channels = load_channels();
            std::string channel_list = "";
            for (const auto& ch : channels) {
                channel_list += ch + ";";
            }
            rep_map["status"] = "success";
            rep_map["channels"] = channel_list;
        }

        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, rep_map);
        zmq::message_t reply(sbuf.data(), sbuf.size());
        socket.send(reply, zmq::send_flags::none);
    }

    return 0;
}