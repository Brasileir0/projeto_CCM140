
import zmq

def main():
    context = zmq.Context()

    xsub = context.socket(zmq.XSUB)
    xsub.bind("tcp://*:5557")

    xpub = context.socket(zmq.XPUB)
    xpub.bind("tcp://*:5558")

    print("[Proxy Pub/Sub Python] Iniciado", flush=True)

    try:
        zmq.proxy(xsub, xpub)
    except KeyboardInterrupt:
        print("[Proxy Pub/Sub Python] Encerrando", flush=True)

    finally:
        xsub.close()
        xpub.close()
        context.term()


if __name__ == "__main__":
    main()