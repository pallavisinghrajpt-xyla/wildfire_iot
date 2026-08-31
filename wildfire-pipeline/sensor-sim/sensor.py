"""
Synthetic wildfire sensor data generator.
Same logic as the original script, but:
  - broker / port / topic prefix come from environment variables
  - topics are namespaced under a unique prefix so nobody else on the
    public HiveMQ broker collides with your data
Publishes to:  {TOPIC_PREFIX}/sensors/raw/{node_id}
"""
import json
import time
import random
import math
import os
from datetime import datetime, timezone

import paho.mqtt.client as mqtt  # paho-mqtt==1.6.1

MQTT_BROKER = os.environ.get("MQTT_BROKER", "broker.hivemq.com")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
TOPIC_PREFIX = os.environ.get("TOPIC_PREFIX", "wildfire-change-me")
TOPIC_BASE = f"{TOPIC_PREFIX}/sensors/raw"
NODES = ["node-01", "node-02", "node-03"]

client = mqtt.Client(client_id="wildfire-sensor-sim")


def connect_with_retry():
    while True:
        try:
            client.connect(MQTT_BROKER, MQTT_PORT, 60)
            print(f"[sim] connected to {MQTT_BROKER}:{MQTT_PORT}", flush=True)
            return
        except Exception as e:
            print(f"[sim] connect failed ({e}); retry in 3s", flush=True)
            time.sleep(3)


def generate_reading(node_id, t, fire_event=False):
    # Base daily temp cycle: cooler at night, warmer mid-afternoon
    hour = (t % 86400) / 3600
    base_temp = 22 + 8 * math.sin((hour - 8) * math.pi / 12)
    base_humidity = 70 - 20 * math.sin((hour - 8) * math.pi / 12)

    if fire_event:
        temp_spike = random.uniform(40, 80)
        smoke_spike = random.uniform(2000, 8000)
        co_spike = random.uniform(400, 1500)
        flame = 1 if random.random() < 0.8 else 0
    else:
        temp_spike = smoke_spike = co_spike = 0
        flame = 1 if random.random() < 0.0001 else 0  # sun false positive

    return {
        "node_id": node_id,
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "temperature": round(base_temp + temp_spike + random.gauss(0, 0.3), 2),
        "humidity": round(max(0, min(100, base_humidity + random.gauss(0, 1))), 2),
        "smoke_ppm": int(350 + smoke_spike + random.gauss(0, 20)),
        "co_ppm": int(30 + co_spike + random.gauss(0, 10)),
        "flame_detected": flame,
        "battery_v": round(random.uniform(3.6, 4.2), 2),
        "rssi": int(random.gauss(-65, 5)),
    }


connect_with_retry()
client.loop_start()

print(f"[sim] publishing to {TOPIC_BASE}/<node>. Ctrl+C to stop", flush=True)
fire_active = False
fire_end_time = 0
fire_node = None

try:
    while True:
        now = time.time()


        if not fire_active and random.random() < 0.012:
            fire_node = random.choice(NODES)
            fire_active = True
            fire_end_time = now + random.uniform(60, 180)
            print(f"[sim] FIRE STARTED on {fire_node}", flush=True)

        if fire_active and now > fire_end_time:
            fire_active = False
            print("[sim] fire ended", flush=True)

        for node in NODES:
            is_fire = fire_active and node == fire_node
            payload = generate_reading(node, now, is_fire)
            client.publish(f"{TOPIC_BASE}/{node}", json.dumps(payload))
            print(f"[sim] {node}: T={payload['temperature']}C, Smoke={payload['smoke_ppm']}", flush=True)

        time.sleep(5)  # an ESP32 would do delay(5000)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
    print("[sim] stopped", flush=True)
