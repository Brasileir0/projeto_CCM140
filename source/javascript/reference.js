const zmq = require('zeromq');
const msgpack = require('msgpack5')();

const serversByName = new Map();
const availableServers = new Map();
let nextRank = 1;
let logicalClock = 0;
const heartbeatTimeoutMs = Number(process.env.HEARTBEAT_TIMEOUT_MS) || 30000;

function getTimestamp() {
    return new Date().toISOString();
}

function observeLogicalClock(value) {
    if (Number.isSafeInteger(value) && value >= 0) {
        logicalClock = Math.max(logicalClock, value);
    }
}

function removeExpiredServers() {
    const now = Date.now();
    for (const [name, server] of availableServers) {
        if (now - server.lastSeen > heartbeatTimeoutMs) {
            availableServers.delete(name);
            console.log(`[Referência] Servidor removido por falta de heartbeat: ${name}`);
        }
    }
}

async function main() {
    const socket = new zmq.Reply();
    const endpoint = process.env.REFERENCE_BIND_ENDPOINT || 'tcp://*:5559';
    await socket.bind(endpoint);
    console.log(`[Referência] Serviço ativo em ${endpoint}`);
    const cleanupTimer = setInterval(removeExpiredServers, 1000);

    try {
        for await (const [requestBuffer] of socket) {
            let request;
            let response;
            try {
                request = msgpack.decode(requestBuffer);
                observeLogicalClock(request && request.logical_clock);

                if (!request || typeof request !== 'object') {
                    response = { status: 'error', message: 'Requisição inválida' };
                } else if (request.action === 'rank' || request.action === 'heartbeat') {
                    const name = typeof request.name === 'string' ? request.name.trim() : '';
                    if (!name) {
                        response = { status: 'error', message: 'Nome do servidor inválido' };
                    } else if (request.action === 'rank') {
                        if (!serversByName.has(name)) {
                            serversByName.set(name, nextRank);
                            nextRank += 1;
                        }
                        availableServers.set(name, {
                            rank: serversByName.get(name),
                            lastSeen: Date.now()
                        });
                        response = { status: 'success', rank: serversByName.get(name) };
                    } else if (!serversByName.has(name)) {
                        response = { status: 'error', message: 'Servidor não registrado' };
                    } else {
                        const rank = serversByName.get(name);
                        availableServers.set(name, { rank, lastSeen: Date.now() });
                        response = { status: 'success', rank };
                    }
                } else if (request.action === 'list') {
                    response = {
                        status: 'success',
                        servers: Array.from(availableServers, ([name, server]) => ({
                            name,
                            rank: server.rank
                        })).sort((left, right) => left.rank - right.rank)
                    };
                } else {
                    response = { status: 'error', message: 'Ação desconhecida' };
                }
            } catch (error) {
                console.error('[Referência] Erro ao processar requisição:', error);
                response = { status: 'error', message: 'Falha ao processar a requisição' };
            }

            logicalClock += 1;
            if (!request || request.action !== 'heartbeat') {
                response.timestamp = getTimestamp();
            }
            response.logical_clock = logicalClock;
            await socket.send(msgpack.encode(response));
        }
    } finally {
        clearInterval(cleanupTimer);
        socket.close();
    }
}

main().catch((error) => {
    console.error('[Referência] Falha fatal:', error);
    process.exitCode = 1;
});
