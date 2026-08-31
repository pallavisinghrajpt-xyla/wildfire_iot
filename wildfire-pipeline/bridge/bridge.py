"""
Custom MQTT -> Kafka bridge.

Subscribes to the public HiveMQ broker under your unique topic prefix,
and republishes each message onto a Kafka topic. Messages are keyed by
node_id so each node's readings land in the same partition and stay
ordered (which matters for Flink's per-node windowing).

    {TOPIC_PREFIX}/sensors/raw/#   --->   Kafka topic  wildfire.sensors.raw
"""
import os
import json
import time

import paho.mqtt.client as mqtt          # paho-mqtt==1.6.1
from confluent_kafka import Producer      # confluent-kafka==2.5.3

MQTT_BROKER = os.environ.get("MQTT_BROKER", "broker.hivemq.com")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "wildfire-change-me")
SUB_TOPIC = f"{TOPIC_PREFIX}/sensors/raw/#"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "wildfire.sensors.raw")


def make_producer():
    """Block until Kafka is reachable, then return a Producer."""
    while True:
        try:
            p = Producer({
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "client.id": "mqtt-kafka-bridge",
                "linger.ms": 50,
            })
            p.list_topics(timeout=5)  # forces a metadata fetch = connectivity check
            print(f"[bridge] connected to Kafka at {KAFKA_BOOTSTRAP}", flush=True)
            return p
        except Exception as e:
            print(f"[bridge] Kafka not ready ({e}); retry in 3s", flush=True)
            time.sleep(3)


producer = make_producer()


def delivery_report(err, msg):
    if err is not None:
        print(f"[bridge] delivery failed: {err}", flush=True)


def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[bridge] MQTT connected (rc={rc}); subscribing to {SUB_TOPIC}", flush=True)
    client.subscribe(SUB_TOPIC, qos=0)


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        try:
            node_id = json.loads(payload).get("node_id", "unknown")
        except Exception:
            node_id = "unknown"
        producer.produce(
            KAFKA_RAW_TOPIC,
            key=node_id,
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)  # serve delivery callbacks
    except Exception as e:
        print(f"[bridge] error handling message: {e}", flush=True)


client = mqtt.Client(client_id="wildfire-bridge", protocol=mqtt.MQTTv311)
client.on_connect = on_connect
client.on_message = on_message

while True:
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        break
    except Exception as e:
        print(f"[bridge] MQTT connect failed ({e}); retry in 3s", flush=True)
        time.sleep(3)

try:
    client.loop_forever()
except KeyboardInterrupt:
    producer.flush(5)
    client.disconnect()
    print("[bridge] stopped", flush=True)
