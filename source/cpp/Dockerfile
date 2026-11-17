FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libzmq3-dev \
    cppzmq-dev \
    libmsgpack-cxx-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

RUN cmake -S . -B build
RUN cmake --build build -j$(nproc)

CMD ["./build/server"]