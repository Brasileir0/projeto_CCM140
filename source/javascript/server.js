const zmq = require('zeromq');
const msgpack = require('msgpack5')();
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.txt');
const CHANNELS_FILE = path.join(DATA_DIR, 'channels.txt');
const EVENTS_FILE = path.join(DATA_DIR, 'events.jsonl');
fs.mkdirSync(DATA_DIR, { recursive: true });

let logicalClock = 0;
let physicalClockOffset = Number(process.env.CLOCK_OFFSET_MS) || 0;
let clientMessagesReceived = 0;
let coordinatorName = null;
let serverRank = null;
let electionInProgress = false;
let serverName;

const CLOCK_SYNC_INTERVAL = 15;
const PEER_PORT = Number(process.env.PEER_PORT) || 5560;

function getTimestamp() {
    return new Date(Date.now() + physicalClockOffset).toISOString();
}

function observeLogicalClock(value) {
    if (Number.isSafeInteger(value) && value >= 0) {
        logicalClock = Math.max(logicalClock, value);
    }
}

function tickLogicalClock() {
    logicalClock += 1;
    return logicalClock;
}

function objectToString(values) {
    return `{ ${Object.keys(values).sort().map((key) => `${key}: ${values[key]}`).join(' | ')} }`;
}

function persistEvent(event) {
    fs.appendFileSync(EVENTS_FILE, `${JSON.stringify(event)}\n`, 'utf8');
}

function saveUser(username, timestamp) {
    fs.appendFileSync(USERS_FILE, `${username},${timestamp}\n`, 'utf8');
}

function saveChannel(channel, timestamp) {
    fs.appendFileSync(CHANNELS_FILE, `${channel}\n`, 'utf8');
    persistEvent({ type: 'channel_created', channel, timestamp });
}

function loadChannels() {
    if (!fs.existsSync(CHANNELS_FILE)) {
        return new Set();
    }

    return new Set(
        fs.readFileSync(CHANNELS_FILE, 'utf8')
            .split(/\r?\n/)
            .map((channel) => channel.trim())
            .filter(Boolean)
    );
}

async function callReference(action, extra = {}) {
    const endpoint = process.env.REFERENCE_ENDPOINT;
    if (!endpoint) {
        return null;
    }

    const socket = new zmq.Request();
    socket.receiveTimeout = Number(process.env.REFERENCE_TIMEOUT_MS) || 2000;
    const request = {
        action,
        ...extra,
        timestamp: getTimestamp(),
        logical_clock: tickLogicalClock()
    };
    
    try {
        socket.connect(endpoint);
        await socket.send(msgpack.encode(request));
        const [replyBuffer] = await socket.receive();
        const response = msgpack.decode(replyBuffer);
        observeLogicalClock(response.logical_clock);
        return response;
    } catch (error) {
        console.error(`[Servidor JS] Falha na comunicação com a referência (${action}):`, error);
        return null;
    } finally {
        socket.close();
    }
}

async function callPeer(peerName, action) {
    const socket = new zmq.Request();
    socket.receiveTimeout = Number(process.env.PEER_TIMEOUT_MS) || 1000;
    socket.sendTimeout = Number(process.env.PEER_TIMEOUT_MS) || 1000;
    const request = {
        action,
        name: serverName,
        timestamp: getTimestamp(),
        logical_clock: tickLogicalClock()
    };

    try {
        socket.connect(`tcp://${peerName}:${PEER_PORT}`);
        await socket.send(msgpack.encode(request));
        const [replyBuffer] = await socket.receive();
        const response = msgpack.decode(replyBuffer);
        observeLogicalClock(response.logical_clock);
        return response;
    } catch (error) {
        console.error(`[Servidor JS] Falha na comunicação com ${peerName} (${action}):`, error);
        return null;
    } finally {
        socket.close();
    }
}

async function handlePeerRequests(peerSocket) {
    for await (const [requestBuffer] of peerSocket) {
        let response;
        try {
            const request = msgpack.decode(requestBuffer);
            observeLogicalClock(request && request.logical_clock);

            if (!request || typeof request !== 'object') {
                response = { status: 'error', message: 'Requisição inválida' };
            } else if (request.action === 'election') {
                response = { status: 'success', message: 'OK' };
            } else if (request.action === 'clock') {
                response = { status: 'success', timestamp: getTimestamp() };
            } else {
                response = { status: 'error', message: 'Ação desconhecida' };
            }
        } catch (error) {
            console.error('[Servidor JS] Erro ao processar requisição de servidor:', error);
            response = { status: 'error', message: 'Falha ao processar a requisição' };
        }

        response.timestamp = response.timestamp || getTimestamp();
        response.logical_clock = tickLogicalClock();
        try {
            await peerSocket.send(msgpack.encode(response));
        } catch (error) {
            console.error('[Servidor JS] Erro ao responder requisição de servidor:', error);
        }
    }
}

async function listenForCoordinatorAnnouncements(subscriber) {
    for await (const frames of subscriber) {
        if (frames.length < 2) {
            continue;
        }

        try {
            const announcement = msgpack.decode(frames[1]);
            observeLogicalClock(announcement && announcement.logical_clock);
            const announcedName = typeof announcement === 'string'
                ? announcement
                : announcement && (announcement.coordinator || announcement.name);
            if (typeof announcedName === 'string' && announcedName.trim()) {
                coordinatorName = announcedName.trim();
                console.log(`[Servidor JS] Coordenador anunciado: ${coordinatorName}`);
            }
        } catch (error) {
            console.error('[Servidor JS] Anúncio de coordenador inválido:', error);
        }
    }
}

async function announceCoordinator(publisher) {
    coordinatorName = serverName;
    const announcement = {
        action: 'coordinator',
        coordinator: serverName,
        timestamp: getTimestamp(),
        logical_clock: tickLogicalClock()
    };
    await publisher.send([Buffer.from('servers'), msgpack.encode(announcement)]);
    console.log(`[Servidor JS] Eleito coordenador: ${serverName}`);
}

async function electCoordinator(publisher) {
    if (electionInProgress) {
        return;
    }
    electionInProgress = true;

    try {
        const directory = await callReference('list');
        if (!directory || directory.status !== 'success' || !Array.isArray(directory.servers)) {
            console.error('[Servidor JS] Não foi possível consultar os servidores para a eleição.');
            return;
        }

        const available = directory.servers.filter((server) =>
            server && typeof server.name === 'string' && Number.isSafeInteger(server.rank)
        );
        const ownEntry = available.find((server) => server.name === serverName);
        if (ownEntry) {
            serverRank = ownEntry.rank;
        }

        const candidates = [{ name: serverName, rank: serverRank || 0 }];
        const peers = available.filter((server) => server.name !== serverName);
        const peerReplies = await Promise.all(peers.map(async (server) => {
            const response = await callPeer(server.name, 'election');
            return response && response.status === 'success' ? server : null;
        }));
        candidates.push(...peerReplies.filter(Boolean));
        candidates.sort((left, right) => left.rank - right.rank);

        const elected = candidates[candidates.length - 1];
        coordinatorName = elected.name;
        if (elected.name === serverName) {
            await new Promise((resolve) => setTimeout(resolve, 100));
            await announceCoordinator(publisher);
        } else {
            console.log(`[Servidor JS] Coordenador disponível: ${coordinatorName}`);
        }
    } finally {
        electionInProgress = false;
    }
}

async function synchronizeClock(publisher) {
    if (!coordinatorName) {
        await electCoordinator(publisher);
    }
    if (!coordinatorName || coordinatorName === serverName) {
        return;
    }

    const sentAt = Date.now() + physicalClockOffset;
    const response = await callPeer(coordinatorName, 'clock');
    const receivedAt = Date.now() + physicalClockOffset;
    const coordinatorTime = response && Date.parse(response.timestamp);
    if (!response || response.status !== 'success' || !Number.isFinite(coordinatorTime)) {
        console.error(`[Servidor JS] Coordenador indisponível: ${coordinatorName}`);
        coordinatorName = null;
        await electCoordinator(publisher);
        return;
    }

    const estimatedCoordinatorTime = coordinatorTime + (receivedAt - sentAt) / 2;
    const adjustment = estimatedCoordinatorTime - receivedAt;
    physicalClockOffset += adjustment;
    console.log(`[Servidor JS] Relógio sincronizado com ${coordinatorName}: ajuste ${adjustment.toFixed(1)} ms`);
}

async function main() {
    const replySocket = new zmq.Reply();
    const peerSocket = new zmq.Reply();
    const publisher = new zmq.Publisher();
    const subscriber = new zmq.Subscriber();
    serverName = process.env.SERVER_NAME || process.env.HOSTNAME || `server-javascript-${process.pid}`;
    if (process.env.BROKER_BACKEND) {
        replySocket.connect(process.env.BROKER_BACKEND);
        console.log(`[Servidor JS] Conectado ao broker em ${process.env.BROKER_BACKEND}`);
    } else {
        await replySocket.bind(process.env.SERVER_ENDPOINT || 'tcp://*:5555');
        console.log('[Servidor JS] Req-Rep ativo na porta 5555.');
    }
    publisher.connect(process.env.PUBSUB_XSUB_ENDPOINT || 'tcp://proxy-pubsub:5557');
    subscriber.connect(process.env.PUBSUB_XPUB_ENDPOINT || 'tcp://proxy-pubsub:5558');
    subscriber.subscribe('servers');
    console.log('[Servidor JS] Publisher conectado ao proxy Pub/Sub');
    const peerEndpoint = process.env.PEER_BIND_ENDPOINT || `tcp://*:${PEER_PORT}`;
    await peerSocket.bind(peerEndpoint);
    console.log(`[Servidor JS] Comunicação entre servidores ativa em ${peerEndpoint}`);
    handlePeerRequests(peerSocket).catch((error) => {
        console.error('[Servidor JS] Falha no serviço entre servidores:', error);
        process.exitCode = 1;
    });
    listenForCoordinatorAnnouncements(subscriber).catch((error) => {
        console.error('[Servidor JS] Falha ao receber anúncios de coordenador:', error);
        process.exitCode = 1;
    });

    const registration = await callReference('rank', { name: serverName });
    if (registration && registration.status === 'success') {
        serverRank = registration.rank;
        console.log(`[Servidor JS] Registrado como ${serverName}, rank ${registration.rank}`);
        await electCoordinator(publisher);
    }

    for await (const [requestBuffer] of replySocket) {
        const receivedAt = getTimestamp();
        let request;
        let response;
        clientMessagesReceived += 1;

        try {
            request = msgpack.decode(requestBuffer);
            observeLogicalClock(request && request.logical_clock);
            persistEvent({ type: 'request', receivedAt, request });

            if (!request || typeof request !== 'object' ||
                typeof request.timestamp !== 'string' || request.timestamp.trim() === '') {
                response = { status: 'error', message: 'Timestamp ausente ou requisição inválida' };
                console.log(`[${receivedAt}][ERRO] Timestamp ausente ou requisição inválida`);
            } else {
                console.log(`[${receivedAt}][RECEBIDO] ${objectToString(request)}`);
                if (request.action === 'login') {
                    const username = typeof request.username === 'string' ? request.username.trim() : '';
                    if (!username) {
                        response = { status: 'error', message: 'Nome de usuário inválido' };
                        console.log(`[${request.timestamp}][LOGIN ERRO] Nome inválido`);
                    } else {
                        saveUser(username, request.timestamp);
                        response = { status: 'success', message: 'Login realizado com sucesso' };
                        console.log(`[${request.timestamp}][LOGIN] Usuário: ${username}`);
                    }
                } else if (request.action === 'create_channel') {
                    const channel = typeof request.channel === 'string' ? request.channel.trim() : '';
                    const channels = loadChannels();
                    if (!channel) {
                        response = { status: 'error', message: 'Nome de canal inválido' };
                        console.log(`[${request.timestamp}][CANAL ERRO] Nome inválido`);
                    } else if (channels.has(channel)) {
                        response = { status: 'error', message: 'Canal já existe' };
                        console.log(`[${request.timestamp}][CANAL ERRO] Já existe: ${channel}`);
                    } else {
                        saveChannel(channel, request.timestamp);
                        response = { status: 'success', message: 'Canal criado com sucesso' };
                        console.log(`[${request.timestamp}][CHANNEL] Canal criado: ${channel}`);
                    }
                } else if (request.action === 'list_channels') {
                    response = {
                        status: 'success',
                        channels: Array.from(loadChannels()).sort().join(';')
                    };
                    console.log(`[${request.timestamp}][CANAIS] Lista enviada: ${response.channels}`);
                } else if (request.action === 'publish') {
                    const channel = typeof request.channel === 'string' ? request.channel.trim() : '';
                    const message = typeof request.message === 'string' ? request.message : '';
                    const username = typeof request.username === 'string' ? request.username.trim() : '';
                    if (!channel || !message.trim()) {
                        response = { status: 'error', message: 'Canal ou mensagem inválidos' };
                        console.log(`[${request.timestamp}][PUB ERRO] Canal ou mensagem inválidos`);
                    } else {
                        if (!loadChannels().has(channel)) {
                            saveChannel(channel, request.timestamp);
                            console.log(`[${request.timestamp}][CHANNEL] Canal registrado a partir de publicação: ${channel}`);
                        }
                        const publication = {
                            channel,
                            message,
                            timestamp: getTimestamp()
                        };
                        logicalClock += 1;
                        publication.logical_clock = logicalClock;
                        if (username) {
                            publication.username = username;
                        }
                        persistEvent({ type: 'publication', ...publication, requestTimestamp: request.timestamp });
                        await publisher.send([Buffer.from(channel), msgpack.encode(publication)]);
                        response = { status: 'success', message: 'Publicação enviada' };
                        console.log(`[${publication.timestamp}][PUB] ${objectToString(publication)}`);
                    }
                } else {
                    response = { status: 'error', message: 'Ação desconhecida' };
                    console.log(`[${request.timestamp}][ERRO] Ação desconhecida: ${request.action}`);
                }
            }
        } catch (error) {
            console.error('[Servidor JS] Erro ao processar/persistir requisição:', error);
            response = { status: 'error', message: 'Falha ao processar a requisição' };
        }

        if (clientMessagesReceived % 10 === 0) {
            let heartbeat = await callReference('heartbeat', { name: serverName });
            if (heartbeat && heartbeat.status !== 'success') {
                console.error(`[Servidor JS] Heartbeat rejeitado: ${heartbeat.message || 'resposta inválida'}`);
                heartbeat = await callReference('rank', { name: serverName });
                if (heartbeat && heartbeat.status === 'success') {
                    serverRank = heartbeat.rank;
                    console.log(`[Servidor JS] Registro recuperado, rank ${heartbeat.rank}`);
                }
            }
        }
        if (clientMessagesReceived % CLOCK_SYNC_INTERVAL === 0) {
            await synchronizeClock(publisher);
        }

        tickLogicalClock();
        response.timestamp = getTimestamp();
        response.logical_clock = logicalClock;
        try {
            persistEvent({ type: 'response', timestamp: response.timestamp, response });
            console.log(`[${response.timestamp}][ENVIADO] ${objectToString(response)}`);
            await replySocket.send(msgpack.encode(response));
        } catch (error) {
            console.error('[Servidor JS] Erro ao persistir/enviar resposta:', error);
        }
    }
}

main().catch((error) => {
    console.error('[Servidor JS] Falha fatal:', error);
    process.exitCode = 1;
});
