FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir pyzmq

COPY broker.py cliente.py servidor.py ./

CMD ["python", "broker.py"]
