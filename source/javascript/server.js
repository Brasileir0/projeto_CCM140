const zmq = require('zeromq');
const msgpack = require('msgpack5')();
const fs = require('fs');
const path = require('path');

const USERS_FILE = path.join(__dirname, 'users.txt');
const CHANNELS_FILE = path.join(__dirname, 'channels.txt');

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
        const timestamp = getTimestamp();
        let repMap = {};

        if (action === 'login') {
            const username = reqMap.username;
            saveUser(username, timestamp);
            console.log(`[${timestamp}][LOGIN] Usuário: ${username}`);
            repMap.status = 'success';
            repMap.message = 'Login realizado com sucesso';
        } 
        else if (action === 'create_channel') {
            const channel = reqMap.channel;
            const channels = loadChannels();
            
            if (!channels.has(channel)) {
                saveChannel(channel);
                console.log(`[${timestamp}][CHANNEL] Canal criado: ${channel}`);
                repMap.status = 'success';
                repMap.message = 'Canal criado com sucesso';
            } else {
                repMap.status = 'error';
                repMap.message = 'Canal já existe';
            }
        } 
        else if (action === 'list_channels') {
            const channels = loadChannels();
            let channelList = '';
            for (const ch of channels) {
                channelList += ch + ';';
            }
            repMap.status = 'success';
            repMap.channels = channelList;
        }

        const replyBuffer = msgpack.encode(repMap);
        await sock.send(replyBuffer);
    }
}

main().catch(err => {
    console.error('Erro no servidor:', err);
});