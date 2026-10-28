import zmq

def main():
    context = zmq.Context()
    
    cliente_socket = context.socket(zmq.ROUTER)
    cliente_socket.bind("tcp://*:5555")
    
    servidor_socket = context.socket(zmq.DEALER)
    servidor_socket.bind("tcp://*:5556")
    
    print("Broker iniciado. Aguardando conexões...")

    try:
        zmq.proxy(cliente_socket, servidor_socket)
    except KeyboardInterrupt:
        print("Broker encerrado.")
    finally:
        cliente_socket.close()
        servidor_socket.close()
        context.term()

if __name__ == "__main__":
    main()
