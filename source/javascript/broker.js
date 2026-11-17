const zmq = require('zeromq');

async function main() {
    const frontend = new zmq.Router();
    const backend = new zmq.Dealer();
    const proxy = new zmq.Proxy(frontend, backend);

    await frontend.bind(process.env.BROKER_FRONTEND || 'tcp://*:5555');
    await backend.bind(process.env.BROKER_BACKEND || 'tcp://*:5556');
    console.log('[Broker JS] Proxy ROUTER/DEALER ativo.');
    await proxy.run();
}

main().catch((error) => {
    console.error('Erro no broker:', error);
    process.exitCode = 1;
});