const zmq = require('zeromq');
const msgpack = require('msgpack5')();
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.txt');
const CHANNELS_FILE = path.join(DATA_DIR, 'channels.txt');
const MESSAGES_FILE = path.join(DATA_DIR, 'messages.txt');
const EVENTS_FILE = path.join(DATA_DIR, 'events.jsonl');
fs.mkdirSync(DATA_DIR, { recursive: true });

let logicalClock = 0;
let physicalClockOffset = Number(process.env.CLOCK_OFFSET_MS) || 0;
let clientMessagesReceived = 0;
let coordinatorName = null;
let serverRank = null;
let electionInProgress = false;
let serverName;
let replicationPrefix;
let replicationSequence = 0;
const appliedReplications = new Set();

const CLOCK_SYNC_INTERVAL = 15;
const PEER_PORT = Number(process.env.PEER_PORT) || 5560;
const REPLICATION_TOPIC = 'replication';

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

function appendUniqueLine(file, line) {
    const lines = fs.existsSync(file)
        ? fs.readFileSync(file, 'utf8').split(/\r?\n/)
        : [];

    if (lines.includes(line)) {
        return false;
    }

    fs.appendFileSync(file, `${line}\n`, 'utf8');
    return true;
}

function saveUser(username, timestamp) {
    return appendUniqueLine(USERS_FILE, `${username},${timestamp}`);
}

function saveChannel(channel, timestamp) {

    if (!appendUniqueLine(CHANNELS_FILE, channel)) {
        return false;
    }
    persistEvent({ type: 'channel_created', channel, timestamp });
    return true;
}

function saveMessage(channel, username, message, requestTimestamp, publicationTimestamp) {
    return appendUniqueLine(
        MESSAGES_FILE,
        `${channel}|${username}|${message}|${requestTimestamp}|${publicationTimestamp}`
    );
}

function migratePublicationEvents() {

    if (!fs.existsSync(EVENTS_FILE)) {
        return;
    }

    let migrated = 0;
    const lines = fs.readFileSync(EVENTS_FILE, 'utf8').split(/\r?\n/).filter(Boolean);
    for (const [index, line] of lines.entries()) {
        let event;
        try {
            event = JSON.parse(line);
        } catch (error) {
            console.error(`[Servidor JS] Evento inválido em ${EVENTS_FILE}:${index + 1}:`, error);
            continue;
        }

        if (!event || typeof event !== 'object') {
            console.error(`[Servidor JS] Evento inválido em ${EVENTS_FILE}:${index + 1}`);
            continue;
        }

        if (event.type === 'publication' &&
            typeof event.channel === 'string' &&
            typeof event.message === 'string' &&
            typeof event.requestTimestamp === 'string' &&
            typeof event.timestamp === 'string') {

            if (saveMessage(
                event.channel,
                typeof event.username === 'string' ? event.username : '',
                event.message,
                event.requestTimestamp,
                event.timestamp
            )) {
                migrated += 1;
            }
        }
    }

    if (migrated) {
        console.log(`[Servidor JS] ${migrated} publicação(ões) anterior(es) recuperada(s) para a replicação`);
    }
}

function dumpLines(file) {

    if (!fs.existsSync(file)) {
        return '';
    }

    const lines = fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean);
    return lines.length ? `${lines.join('\n')}\n` : '';
}

function mergeLines(file, contents) {

    if (typeof contents !== 'string') {
        return 0;
    }

    let added = 0;
    for (const line of contents.split(/\r?\n/).filter(Boolean)) {
        if (appendUniqueLine(file, line)) {
            added += 1;
        }
    }
    return added;
}

async function publishReplication(publisher, fields) {
    const replication = {
        ...fields,
        origin: serverName,
        id: `${replicationPrefix}:${++replicationSequence}`,
        timestamp: getTimestamp(),
        logical_clock: tickLogicalClock()
    };
    await publisher.send([Buffer.from(REPLICATION_TOPIC), msgpack.encode(replication)]);
    console.log(`[${replication.timestamp}][REPLICAÇÃO] Enviado: ${objectToString(replication)}`);
}

function applyReplication(replication) {
    const origin = typeof replication.origin === 'string' ? replication.origin : '';
    const id = typeof replication.id === 'string' ? replication.id : '';

    if (origin === serverName || (id && appliedReplications.has(id))) {
        return;
    }

    const type = replication.type;
    let stored = false;

    if (type === 'login' &&
        typeof replication.username === 'string' &&
        typeof replication.login_timestamp === 'string') {
        stored = saveUser(replication.username, replication.login_timestamp);
    } else if (type === 'channel' && typeof replication.channel === 'string') {
        stored = saveChannel(replication.channel, replication.timestamp);
    } else if (type === 'publication' &&
        typeof replication.channel === 'string' &&
        typeof replication.username === 'string' &&
        typeof replication.message === 'string' &&
        typeof replication.request_timestamp === 'string' &&
        typeof replication.publication_timestamp === 'string') {
        saveChannel(replication.channel, replication.publication_timestamp);
        stored = saveMessage(
            replication.channel,
            replication.username,
            replication.message,
            replication.request_timestamp,
            replication.publication_timestamp
        );
    } else {
        console.error(`[${getTimestamp()}][REPLICAÇÃO] Mensagem inválida recebida de ${origin}`);
        return;
    }

    if (id) {
        appliedReplications.add(id);
    }
    console.log(`[${getTimestamp()}][REPLICAÇÃO] Recebido de ${origin} (${type}, ${stored ? 'gravado' : 'já existia'})`);
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
            } else if (request.action === 'sync') {
                response = {
                    status: 'success',
                    users: dumpLines(USERS_FILE),
                    channels: dumpLines(CHANNELS_FILE),
                    messages: dumpLines(MESSAGES_FILE)
                };
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

async function listenForPubSubMessages(subscriber) {
    for await (const frames of subscriber) {
        if (frames.length < 2) {
            continue;
        }

        try {
            const topic = frames[0].toString();
            const message = msgpack.decode(frames[1]);
            observeLogicalClock(message && message.logical_clock);

            if (topic === 'servers') {
                const announcedName = typeof message === 'string'
                    ? message
                    : message && (message.coordinator || message.name);

                if (typeof announcedName === 'string' && announcedName.trim()) {
                    coordinatorName = announcedName.trim();
                    console.log(`[Servidor JS] Coordenador anunciado: ${coordinatorName}`);
                }
            } else if (topic === REPLICATION_TOPIC && message && typeof message === 'object') {
                applyReplication(message);
            }
        } catch (error) {
            console.error('[Servidor JS] Mensagem Pub/Sub inválida:', error);
        }
    }
}

async function synchronizeInitialData() {
    const directory = await callReference('list');

    if (!directory || directory.status !== 'success' || !Array.isArray(directory.servers)) {
        console.error('[Servidor JS] Não foi possível consultar os servidores para a sincronização inicial.');
        return;
    }

    for (const server of directory.servers) {

        if (!server || typeof server.name !== 'string' || server.name === serverName) {
            continue;
        }

        const response = await callPeer(server.name, 'sync');

        if (!response || response.status !== 'success') {
            continue;
        }

        const users = mergeLines(USERS_FILE, response.users);
        const channels = mergeLines(CHANNELS_FILE, response.channels);
        const messages = mergeLines(MESSAGES_FILE, response.messages);
        console.log(`[${getTimestamp()}][REPLICAÇÃO] Sincronização inicial com ${server.name}: ${users} login(s), ${channels} canal(is) e ${messages} mensagem(ns) novos`);
        return;
    }

    console.error('[Servidor JS] Nenhum servidor respondeu à sincronização inicial.');
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
    replicationPrefix = `${serverName}:${Date.now()}:${process.pid}`;
    migratePublicationEvents();

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
    subscriber.subscribe(REPLICATION_TOPIC);
    console.log('[Servidor JS] Publisher conectado ao proxy Pub/Sub');
    const peerEndpoint = process.env.PEER_BIND_ENDPOINT || `tcp://*:${PEER_PORT}`;
    await peerSocket.bind(peerEndpoint);
    console.log(`[Servidor JS] Comunicação entre servidores ativa em ${peerEndpoint}`);
    handlePeerRequests(peerSocket).catch((error) => {
        console.error('[Servidor JS] Falha no serviço entre servidores:', error);
        process.exitCode = 1;
    });
    listenForPubSubMessages(subscriber).catch((error) => {
        console.error('[Servidor JS] Falha ao receber mensagens Pub/Sub:', error);
        process.exitCode = 1;
    });

    const registration = await callReference('rank', { name: serverName });
    if (registration && registration.status === 'success') {
        serverRank = registration.rank;
        console.log(`[Servidor JS] Registrado como ${serverName}, rank ${registration.rank}`);
        await new Promise((resolve) => setTimeout(resolve, 1000));
        await synchronizeInitialData();
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
                        await publishReplication(publisher, {
                            type: 'login',
                            username,
                            login_timestamp: request.timestamp
                        });
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
                        await publishReplication(publisher, { type: 'channel', channel });
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
                        saveMessage(channel, username, message, request.timestamp, publication.timestamp);
                        logicalClock += 1;
                        publication.logical_clock = logicalClock;
                        if (username) {
                            publication.username = username;
                        }
                        persistEvent({ type: 'publication', ...publication, requestTimestamp: request.timestamp });
                        await publisher.send([Buffer.from(channel), msgpack.encode(publication)]);
                        response = { status: 'success', message: 'Publicação enviada' };
                        console.log(`[${publication.timestamp}][PUB] ${objectToString(publication)}`);
                        await publishReplication(publisher, {
                            type: 'publication',
                            channel,
                            username,
                            message,
                            request_timestamp: request.timestamp,
                            publication_timestamp: publication.timestamp
                        });
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
