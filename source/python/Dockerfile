FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY broker.py client.py server.py proxy_pubsub.py reference.py ./

CMD ["python", "server.py"]