"""
Wildfire detection — PyFlink DataStream job.

Pipeline inside Flink:

  Kafka source (raw JSON)
     -> parse to dict + attach event-time from the sensor's own timestamp
     -> [sink A] write EVERY reading to InfluxDB  (measurement: sensor_readings)
     -> keyBy(node_id)
     -> tumbling event-time window (WINDOW_SECONDS)
     -> sustained detection: alert only if >= MIN_BREACHES readings in the
        window have temperature AND smoke both over threshold
     -> [sink B] write alerts to InfluxDB          (measurement: fire_alerts)

Config values are read here (on the client/driver) and passed into the
operator constructors, so they travel with the job to the TaskManager.
The elements flow as plain Python dicts (PICKLED_BYTE_ARRAY) to keep the
type handling simple and robust at this data volume.
"""
import os
import json
from datetime import datetime

from pyflink.common import Types, WatermarkStrategy, Duration, Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

# ---------------- config (read on the driver, shipped to operators) ----------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_RAW_TOPIC = os.environ.get("KAFKA_RAW_TOPIC", "wildfire.sensors.raw")
INFLUX_URL = os.environ.get("INFLUX_URL", "http://influxdb:8086")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "wildfire")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "sensors")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "wildfire-admin-token-change-me")
TEMP_THRESHOLD = float(os.environ.get("TEMP_THRESHOLD", "45"))
SMOKE_THRESHOLD = float(os.environ.get("SMOKE_THRESHOLD", "1500"))
WINDOW_SECONDS = int(os.environ.get("WINDOW_SECONDS", "30"))
MIN_BREACHES = int(os.environ.get("MIN_BREACHES", "3"))


def _iso_to_ms(ts: str) -> int:
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)


class ParseReading(MapFunction):
    """Kafka JSON string -> dict, adding an epoch-ms field for event time."""
    def map(self, value):
        d = json.loads(value)
        d["ts_ms"] = _iso_to_ms(d["timestamp"])
        return d


class ReadingTsAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value["ts_ms"]


class InfluxReadingWriter(MapFunction):
    """Sink A — writes every reading to InfluxDB as it flows through."""
    def __init__(self, url, token, org, bucket):
        self.url, self.token, self.org, self.bucket = url, token, org, bucket

    def open(self, runtime_context):
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS
        self._client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self._w = self._client.write_api(write_options=SYNCHRONOUS)

    def map(self, d):
        from influxdb_client import Point, WritePrecision
        p = (Point("sensor_readings")
             .tag("node_id", d["node_id"])
             .field("temperature", float(d["temperature"]))
             .field("humidity", float(d["humidity"]))
             .field("smoke_ppm", int(d["smoke_ppm"]))
             .field("co_ppm", int(d["co_ppm"]))
             .field("flame_detected", int(d["flame_detected"]))
             .field("battery_v", float(d["battery_v"]))
             .field("rssi", int(d["rssi"]))
             .time(int(d["ts_ms"]), WritePrecision.MS))
        self._w.write(bucket=self.bucket, record=p)
        return f"stored {d['node_id']} T={d['temperature']}"


class FireDetector(ProcessWindowFunction):
    """Sustained detection: count breaching readings inside one node's window."""
    def __init__(self, temp_th, smoke_th, min_breaches):
        self.temp_th, self.smoke_th, self.min_breaches = temp_th, smoke_th, min_breaches

    def process(self, key, context, elements):
        breaches = 0
        max_t = -999.0
        max_s = 0
        for e in elements:
            t = float(e["temperature"])
            s = int(e["smoke_ppm"])
            if t > self.temp_th and s > self.smoke_th:
                breaches += 1
            max_t = max(max_t, t)
            max_s = max(max_s, s)
        if breaches >= self.min_breaches:
            yield {
                "node_id": key,
                "window_end_ms": context.window().end,
                "breach_count": breaches,
                "max_temp": max_t,
                "max_smoke": max_s,
            }


class InfluxAlertWriter(MapFunction):
    """Sink B — writes fire alerts to InfluxDB."""
    def __init__(self, url, token, org, bucket):
        self.url, self.token, self.org, self.bucket = url, token, org, bucket

    def open(self, runtime_context):
        from influxdb_client import InfluxDBClient
        from influxdb_client.client.write_api import SYNCHRONOUS
        self._client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        self._w = self._client.write_api(write_options=SYNCHRONOUS)

    def map(self, a):
        from influxdb_client import Point, WritePrecision
        msg = (f"FIRE on {a['node_id']}: {a['breach_count']} sustained breaches, "
               f"peak {a['max_temp']:.1f}C / {a['max_smoke']} ppm")
        p = (Point("fire_alerts")
             .tag("node_id", a["node_id"])
             .field("breach_count", int(a["breach_count"]))
             .field("max_temp", float(a["max_temp"]))
             .field("max_smoke", int(a["max_smoke"]))
             .field("message", msg)
             .time(int(a["window_end_ms"]), WritePrecision.MS))
        self._w.write(bucket=self.bucket, record=p)
        print("[flink] " + msg)
        return msg


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # simple + correct for this volume; raise later if needed

    source = (KafkaSource.builder()
              .set_bootstrap_servers(KAFKA_BOOTSTRAP)
              .set_topics(KAFKA_RAW_TOPIC)
              .set_group_id("flink-wildfire")
              .set_starting_offsets(KafkaOffsetsInitializer.latest())
              .set_value_only_deserializer(SimpleStringSchema())
              .build())

    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    # parse, then attach event-time watermarks from the sensor timestamp
    wm = (WatermarkStrategy
          .for_bounded_out_of_orderness(Duration.of_seconds(10))
          .with_timestamp_assigner(ReadingTsAssigner())
          .with_idleness(Duration.of_seconds(15)))

    readings = (raw
                .map(ParseReading(), output_type=Types.PICKLED_BYTE_ARRAY())
                .assign_timestamps_and_watermarks(wm))

    # Sink A: store every reading. The trailing .print() is the terminal that
    # makes Flink actually run this branch (and doubles as a low-volume heartbeat).
    (readings
     .map(InfluxReadingWriter(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET),
          output_type=Types.STRING())
     .name("influx-readings-sink")
     .print())

    # Sink B: windowed sustained detection -> alerts -> InfluxDB
    alerts = (readings
              .key_by(lambda d: d["node_id"])
              .window(TumblingEventTimeWindows.of(Time.seconds(WINDOW_SECONDS)))
              .process(FireDetector(TEMP_THRESHOLD, SMOKE_THRESHOLD, MIN_BREACHES),
                       output_type=Types.PICKLED_BYTE_ARRAY()))

    (alerts
     .map(InfluxAlertWriter(INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET),
          output_type=Types.STRING())
     .name("influx-alerts-sink")
     .print())

    env.execute("wildfire-detection")


if __name__ == "__main__":
    main()
