const zmq = require('zeromq');
const msgpack = require('msgpack5')();

function getTimestamp() {
    const now = new Date();
    const pad = (value) => String(value).padStart(2, '0');
    return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ` +
        `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

async function sendRequest(socket, requestData, responseField) {
    const timestamp = getTimestamp();
    const request = { ...requestData, timestamp };
    console.log(`[${timestamp}][REQ] ${request.action}`);
    await socket.send(msgpack.encode(request));
    const [replyBuffer] = await socket.receive();
    const response = msgpack.decode(replyBuffer);
    console.log(`[${timestamp}][REP] ${response[responseField] || ''}`);
    return response;
}

async function main() {
    const endpoints = (process.env.SERVER_ENDPOINTS || 'tcp://server-javascript:5555')
        .split(',')
        .map((endpoint) => endpoint.trim())
        .filter(Boolean);

    for (const endpoint of endpoints) {
        console.log(`[Cliente JS] Conectando a ${endpoint}`);
        const socket = new zmq.Request();
        socket.connect(endpoint);

        try {
            const loginRequest = { action: 'login', username: 'JAVASCRIPT' };
            let login = await sendRequest(socket, loginRequest, 'message');
            while (login.status !== 'success') {
                await new Promise((resolve) => setTimeout(resolve, 1000));
                login = await sendRequest(socket, loginRequest, 'message');
            }

            await sendRequest(
                socket,
                { action: 'create_channel', channel: 'geral' },
                'message'
            );
            await sendRequest(socket, { action: 'list_channels' }, 'channels');
        } finally {
            await socket.close();
        }
    }
}

main();
