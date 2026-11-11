const zmq = require('zeromq');
const msgpack = require('msgpack5')();

function getTimestamp() {
    return new Date().toISOString();
}

function delay(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function chooseRandom(items) {
    return items[Math.floor(Math.random() * items.length)];
}

async function sendRequest(socket, requestData) {
    const request = { ...requestData, timestamp: getTimestamp() };
    console.log(`[${request.timestamp}][REQ] ${request.action}`);
    await socket.send(msgpack.encode(request));
    const [replyBuffer] = await socket.receive();
    const response = msgpack.decode(replyBuffer);
    console.log(`[${response.timestamp}][REP] ${response.message || response.status}`);
    return response;
}

async function receivePublications(socket, subscribedChannels) {
    for await (const frames of socket) {
        if (frames.length < 2) {
            console.error('[Cliente JS] Mensagem Pub/Sub incompleta recebida.');
            continue;
        }

        const channel = frames[0].toString();
        if (!subscribedChannels.has(channel)) {
            continue;
        }

        try {
            const publication = msgpack.decode(frames[1]);
            console.log(
                `[PUB][canal=${channel}][enviada=${publication.timestamp}]` +
                `[recebida=${getTimestamp()}] ${publication.message}`
            );
        } catch (error) {
            console.error(`[Cliente JS] Publicação inválida no canal ${channel}:`, error);
        }
    }
}

async function ensureChannel(socket, channels, username) {
    if (!Array.isArray(channels)) {
        throw new Error('A resposta do servidor não contém uma lista de canais válida.');
    }

    if (channels.length >= 5) {
        return channels;
    }

    const channel = `canal-${username}-${Date.now()}`;
    const response = await sendRequest(socket, { action: 'create_channel', channel });
    if (response.status !== 'success') {
        throw new Error(`Não foi possível criar o canal ${channel}: ${response.message}`);
    }

    const updated = await sendRequest(socket, { action: 'list_channels' });
    if (updated.status !== 'success' || !Array.isArray(updated.channels)) {
        throw new Error('Não foi possível atualizar a lista de canais.');
    }
    return updated.channels;
}

async function main() {
    const endpoints = (process.env.SERVER_ENDPOINTS || 'tcp://server-javascript-1:5555')
        .split(',')
        .map((endpoint) => endpoint.trim())
        .filter(Boolean);
    if (endpoints.length === 0) {
        throw new Error('SERVER_ENDPOINTS não contém endpoints válidos.');
    }

    const endpoint = chooseRandom(endpoints);
    const username = process.env.BOT_NAME || `JAVASCRIPT-${process.pid}`;
    const requestSocket = new zmq.Request();
    const subscriber = new zmq.Subscriber();
    const subscribedChannels = new Set();
    requestSocket.connect(endpoint);
    subscriber.connect(process.env.PUBSUB_XPUB_ENDPOINT || 'tcp://proxy-pubsub:5558');
    const receiver = receivePublications(subscriber, subscribedChannels).catch((error) => {
        if (error.code !== 'ETERM') {
            console.error('[Cliente JS] Erro no recebimento Pub/Sub:', error);
            process.exitCode = 1;
        }
    });

    try {
        console.log(`[Cliente JS] Conectado a ${endpoint} como ${username}`);
        const login = await sendRequest(requestSocket, { action: 'login', username });
        if (login.status !== 'success') {
            throw new Error(`Login falhou: ${login.message}`);
        }

        const channelList = await sendRequest(requestSocket, { action: 'list_channels' });
        if (channelList.status !== 'success') {
            throw new Error(`Não foi possível listar canais: ${channelList.message}`);
        }
        let { channels } = channelList;
        channels = await ensureChannel(requestSocket, channels, username);
        if (channels.length === 0) {
            throw new Error('O servidor não retornou canais disponíveis.');
        }

        const subscriptions = [...channels]
            .sort(() => Math.random() - 0.5)
            .slice(0, Math.min(3, channels.length));
        for (const channel of subscriptions) {
            subscribedChannels.add(channel);
            subscriber.subscribe(channel);
            console.log(`[Cliente JS] Inscrito no canal ${channel}`);
        }

        while (true) {
            const channel = chooseRandom(channels);
            for (let index = 0; index < 10; index += 1) {
                const message = `Mensagem ${index + 1} de ${username}: ${Math.random().toString(36).slice(2, 10)}`;
                const response = await sendRequest(requestSocket, {
                    action: 'publish',
                    channel,
                    message
                });
                if (response.status !== 'success') {
                    console.error(`[Cliente JS] Publicação falhou: ${response.message}`);
                }
                await delay(1000);
            }
        }
    } finally {
        subscriber.close();
        requestSocket.close();
        await receiver;
    }
}

main().catch((error) => {
    console.error('[Cliente JS] Falha:', error);
    process.exitCode = 1;
});
