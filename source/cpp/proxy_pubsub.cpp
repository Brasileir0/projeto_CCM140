#include <iostream>
#include <zmq.hpp>

int main() {
    zmq::context_t context(1);
    zmq::socket_t xsub(context, zmq::socket_type::xsub);
    zmq::socket_t xpub(context, zmq::socket_type::xpub);

    xsub.bind("tcp://*:5557");
    xpub.bind("tcp://*:5558");

    std::cout << "[PROXY Pub/Sub] XSUB ativo na porta 5557, XPUB ativo na porta 5558";
    
    zmq::proxy(xsub, xpub);
    return 0;
}