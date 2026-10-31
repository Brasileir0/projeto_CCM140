const zmq = require('zeromq');
const msgpack = require('msgpack5')();

function getTimestamp() {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, '0');

    return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ` +
        `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
}

async function sendRequest(socket, requestData) {
    await socket.send(msgpack.encode(requestData));
    const [replyBuffer] = await socket.receive();
    return msgpack.decode(replyBuffer);
}

async function main() {
    const socket = new zmq.Request();
    socket.connect('tcp://server-javascript:5555');
    const timestamp = getTimestamp();

    try {
        const login = await sendRequest(socket, {
            action: 'login',
            username: 'JAVASCRIPT',
            timestamp
        });
        console.log(`[${timestamp}][LOGIN RESP] ${login.message || ''}`);

        const channel = await sendRequest(socket, {
            action: 'create_channel',
            channel: 'geral',
            timestamp
        });
        console.log(`[${timestamp}][CHANNEL RESP] ${channel.message || ''}`);

        const channels = await sendRequest(socket, {
            action: 'list_channels',
            timestamp
        });
        console.log(`[${timestamp}][CANAIS] ${channels.channels || ''}`);
    } catch (error) {
        console.error('Erro na comunicação:', error);
        process.exitCode = 1;
    } finally {
        await socket.close();
    }
}

main();
