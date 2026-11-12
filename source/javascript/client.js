const zmq = require('zeromq');
const msgpack = require('msgpack5')();

function getTimestamp() {
    return new Date().toISOString();
}

function formatTimestamp(timestamp) {
    if (typeof timestamp !== 'string') {
        return 'desconhecido';
    }
    const match = timestamp.match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2})/);
    return match ? `${match[1]}, ${match[2]}` : timestamp;
}

function delay(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function chooseRandom(items) {
    return items[Math.floor(Math.random() * items.length)];
}

function normalizeChannels(channels) {
    if (Array.isArray(channels)) {
        return channels.filter((channel) => typeof channel === 'string' && channel.length > 0);
    }
    if (typeof channels === 'string') {
        return channels.split(';').map((channel) => channel.trim()).filter(Boolean);
    }
    throw new Error('A resposta do servidor não contém uma lista de canais válida.');
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
                `\n---------- NOVA MENSAGEM ---------\n` +
                `Canal: ${channel}\n` +
                `Mensagem: ${publication.message}\n` +
                `Enviada em: ${formatTimestamp(publication.timestamp)}\n` +
                `Recebida em: ${formatTimestamp(getTimestamp())}\n` +
                `------------------------------------\n`
            );
        } catch (error) {
            console.error(`[Cliente JS] Publicação inválida no canal ${channel}:`, error);
        }
    }
}

async function ensureChannel(socket, channels, username) {
    channels = normalizeChannels(channels);
    if (channels.length >= 5) {
        return channels;
    }

    const channel = `canal-${username}-${Date.now()}`;
    const response = await sendRequest(socket, { action: 'create_channel', channel });
    if (response.status !== 'success') {
        throw new Error(`Não foi possível criar o canal ${channel}: ${response.message}`);
    }

    const updated = await sendRequest(socket, { action: 'list_channels' });
    if (updated.status !== 'success') {
        throw new Error('Não foi possível atualizar a lista de canais.');
    }
    return normalizeChannels(updated.channels);
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
        let channels = normalizeChannels(channelList.channels);
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
        await delay(250);

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
