const zmq = require('zeromq');

async function main() {
    const publishers = new zmq.XSubscriber();
    const subscribers = new zmq.XPublisher();
    const proxy = new zmq.Proxy(publishers, subscribers);

    await publishers.bind(process.env.PUBSUB_XSUB_ENDPOINT || 'tcp://*:5557');
    await subscribers.bind(process.env.PUBSUB_XPUB_ENDPOINT || 'tcp://*:5558');
    console.log('[Proxy Pub/Sub JS] XSUB ativo na porta 5557; XPUB ativo na porta 5558.');
    await proxy.run();
}

main().catch((error) => {
    console.error('[Proxy Pub/Sub JS] Falha fatal:', error);
    process.exitCode = 1;
});
