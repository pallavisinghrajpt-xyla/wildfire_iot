FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
# confluent-kafka wheels bundle librdkafka, so no apt packages are needed
RUN pip install --no-cache-dir -r requirements.txt
COPY bridge.py .
CMD ["python", "-u", "bridge.py"]
