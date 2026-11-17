FROM node:22-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package*.json ./

RUN npm install

COPY . /app

# Comando para rodar o cliente JS
CMD ["node", "server.js"]