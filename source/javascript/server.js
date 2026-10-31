const zmq = require('zeromq');
const msgpack = require('msgpack5')();
const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, 'data');
const USERS_FILE = path.join(DATA_DIR, 'users.txt');
const CHANNELS_FILE = path.join(DATA_DIR, 'channels.txt');
fs.mkdirSync(DATA_DIR, { recursive: true });

function getTimestamp() {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, '0');
    
    const year = now.getFullYear();
    const month = pad(now.getMonth() + 1);
    const day = pad(now.getDate());
    const hours = pad(now.getHours());
    const minutes = pad(now.getMinutes());
    const seconds = pad(now.getSeconds());

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
}

function saveUser(username, timestamp) {
    const line = `${username},${timestamp}\n`;
    fs.appendFileSync(USERS_FILE, line, 'utf8');
}

function saveChannel(channel) {
    const line = `${channel}\n`;
    fs.appendFileSync(CHANNELS_FILE, line, 'utf8');
}

function loadChannels() {
    const channels = new Set();
    if (fs.existsSync(CHANNELS_FILE)) {
        const data = fs.readFileSync(CHANNELS_FILE, 'utf8');
        const lines = data.split('\n');
        for (const line of lines) {
            const trimmed = line.trim();
            if (trimmed !== '') {
                channels.add(trimmed);
            }
        }
    }
    return channels;
}

async function main() {
    const sock = new zmq.Reply();

    await sock.bind('tcp://*:5555');
    console.log('[Servidor JS] Iniciado na porta 5555...');

    for await (const [requestBuffer] of sock) {
        const reqMap = msgpack.decode(requestBuffer);
        const action = reqMap.action;
        const requestTimestamp = reqMap.timestamp;
        let repMap = {};

        if (typeof requestTimestamp !== 'string' || requestTimestamp.trim() === '') {
            repMap.status = 'error';
            repMap.message = 'Timestamp ausente';
            console.log(`[${getTimestamp()}][ERRO] Timestamp ausente`);
        } else if (action === 'login') {
            const username = typeof reqMap.username === 'string' ? reqMap.username.trim() : '';
            if (username === '') {
                repMap.status = 'error';
                repMap.message = 'Nome de usuário inválido';
                console.log(`[${requestTimestamp}][LOGIN ERRO] Nome inválido`);
            } else {
                saveUser(username, requestTimestamp);
                console.log(`[${requestTimestamp}][LOGIN] Usuário: ${username}`);
                repMap.status = 'success';
                repMap.message = 'Login realizado com sucesso';
            }
        } else if (action === 'create_channel') {
            const channel = typeof reqMap.channel === 'string' ? reqMap.channel.trim() : '';
            const channels = loadChannels();

            if (channel === '') {
                repMap.status = 'error';
                repMap.message = 'Nome de canal inválido';
                console.log(`[${requestTimestamp}][CANAL ERRO] Nome inválido`);
            } else if (!channels.has(channel)) {
                saveChannel(channel);
                console.log(`[${requestTimestamp}][CHANNEL] Canal criado: ${channel}`);
                repMap.status = 'success';
                repMap.message = 'Canal criado com sucesso';
            } else {
                repMap.status = 'error';
                repMap.message = 'Canal já existe';
                console.log(`[${requestTimestamp}][CANAL ERRO] Já existe: ${channel}`);
            }
        } else if (action === 'list_channels') {
            repMap.status = 'success';
            repMap.channels = Array.from(loadChannels()).sort().join(';');
            console.log(`[${requestTimestamp}][CANAIS] Lista enviada: ${repMap.channels}`);
        } else {
            repMap.status = 'error';
            repMap.message = 'Ação desconhecida';
            console.log(`[${requestTimestamp}][ERRO] Ação desconhecida: ${action}`);
        }

        repMap.timestamp = getTimestamp();
        const replyBuffer = msgpack.encode(repMap);
        await sock.send(replyBuffer);
    }
}

main().catch(err => {
    console.error('Erro no servidor:', err);
});