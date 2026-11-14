const zmq = require('zeromq');
const msgpack = require('msgpack5')();
const fs = require('fs');
const path = require('path');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.txt');
const CHANNELS_FILE = path.join(DATA_DIR, 'channels.txt');
const EVENTS_FILE = path.join(DATA_DIR, 'events.jsonl');
fs.mkdirSync(DATA_DIR, { recursive: true });

function getTimestamp() {
    return new Date().toISOString();
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

async function main() {
    const replySocket = new zmq.Reply();
    const publisher = new zmq.Publisher();
    await replySocket.bind(process.env.SERVER_ENDPOINT || 'tcp://*:5555');
    publisher.connect(process.env.PUBSUB_XSUB_ENDPOINT || 'tcp://proxy-pubsub:5557');
    console.log('[Servidor JS] Req-Rep ativo na porta 5555; Pub conectado ao proxy.');

    for await (const [requestBuffer] of replySocket) {
        const receivedAt = getTimestamp();
        let request;
        let response;

        try {
            request = msgpack.decode(requestBuffer);
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
                    } else if (!loadChannels().has(channel)) {
                        response = { status: 'error', message: 'Canal não existe' };
                        console.log(`[${request.timestamp}][PUB ERRO] Canal não existe: ${channel}`);
                    } else {
                        const publication = {
                            channel,
                            message,
                            timestamp: getTimestamp()
                        };
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

        response.timestamp = getTimestamp();
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
