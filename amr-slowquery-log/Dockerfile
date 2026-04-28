FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY exporter.py .

# /data is mounted as a PersistentVolume in Kubernetes
RUN mkdir -p /data && chown nobody:nogroup /data
ENV OUTPUT_FILE=/data/slowquery.jsonl
ENV STATE_FILE=/data/.slowquery_state.json

USER nobody
CMD ["python", "-u", "exporter.py"]
