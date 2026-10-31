#include <iostream>
#include <zmq.hpp>

int main() {
    zmq::context_t context(1);
    zmq::socket_t frontend(context, zmq::socket_type::router);
    zmq::socket_t backend(context, zmq::socket_type::dealer);

    frontend.bind("tcp://*:5555");
    backend.bind("tcp://*:5556");
    std::cout << "[Broker C++] Proxy ROUTER/DEALER ativo." << std::endl;

    zmq::proxy(frontend, backend);
    return 0;
}