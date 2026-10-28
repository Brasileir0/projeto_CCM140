const zm1 = require('zeromq');
const msgpack = require('msgpack5');

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


async function sendRequest(sock, requestData) {
    const packed = msgpacj.encode(requestData);
    
    await sock.send(packed);

    const [replyBuffer] = await sock.receive();
    return msgpack.Decoder(replyBuffer);
}

async function main() {
    const sock = new zmq.Request();

    sock.connect('tcp://server:5555');
    const timestamp = getTimestamp();

    try{
        const loginReq = {
            action: 'login',
            username: 'JS',
            timestamp: timestamp
        };
        const loginRep = await sendRequest(sock, loginReq);
        console.log(`[${timestamp}][LOGIN RESP]${loginRep.message || ''}`);

        const createReq = {
            action: 'create_channel',
            channel: 'geral',
            timestamp = timestamp
        };
        const createRep = await sendRequest(sock, createReq);
        console.log(`[${timestamp}][CHANNEL RESP] ${createRep.message || ''}`);

        const listReq = {
            action: 'list_channels',
            timestamp: timestamp
        };
        const listRep = await sendRequest(sock, listReq);
        console.log(`[${timestamp}][CANAIS] ${listRep.channel || ''}`);
    } catch (error){
        console.error('Erro na comunicação: ', error);
    } finally {
        await sock.close();
    }
}

main();