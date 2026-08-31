# Wildfire Detection Pipeline

A containerised streaming pipeline for IoT wildfire sensors:

```
sensor-sim ─┐
            ├─(MQTT, public HiveMQ) → bridge → Kafka → Flink → InfluxDB → Grafana
ESP32 later ┘
```

- **sensor-sim** – generates synthetic readings and publishes to the public HiveMQ broker
- **bridge** – custom Python service: subscribes to MQTT, produces to Kafka (keyed by node)
- **Kafka** – durable, replayable log (KRaft mode, no Zookeeper)
- **Flink (PyFlink)** – windowed *sustained* fire detection; writes readings **and** alerts to InfluxDB
- **InfluxDB v2** – time-series storage
- **Grafana** – dashboards + alert table

## Prerequisites
- Docker + Docker Compose v2 (`docker compose` command)
- ~4 GB free RAM (Flink + Kafka + InfluxDB)

## 1. Configure
Open `.env` and **change `TOPIC_PREFIX`** to something unique to you, e.g.
`wildfire-yourname-7h3k9`. This is the single most important step: the public
broker is shared with the whole world, and a unique prefix stops other people's
test traffic from polluting your pipeline (and vice versa). Also change
`INFLUX_TOKEN` if you like.

## 2. Run
```bash
docker compose up --build
```
First build takes a few minutes (Flink installs PyFlink and downloads the Kafka
connector). Watch for `[submitter] job submitted.`

## 3. Open the UIs
| Service       | URL                     | Login          |
|---------------|-------------------------|----------------|
| Grafana       | http://localhost:3000   | admin / admin  |
| Flink Web UI  | http://localhost:8081   | –              |
| InfluxDB      | http://localhost:8086   | admin / admin12345 |

The **Wildfire Detection** dashboard in Grafana is pre-provisioned: temperature
and smoke per node, an alert counter, and a recent-alerts table. The simulator
triggers a fire roughly every ~40 minutes; to see an alert sooner you can lower
`MIN_BREACHES` / thresholds in `.env`, or restart the `sensor-sim` a few times.

## 4. Verify data is flowing
```bash
docker compose logs -f bridge        # should show messages being produced
docker compose logs -f flink-taskmanager   # "stored node-0x ..." heartbeats, and [flink] FIRE ... on alerts
```

## How the detection works
Flink groups each node's readings into tumbling event-time windows
(`WINDOW_SECONDS`) using the sensor's own timestamp. Within a window it counts
readings where temperature **and** smoke both exceed thresholds. Only if at least
`MIN_BREACHES` readings breach does it emit an alert — so a single sun/heat spike
is debounced, while a *sustained* signal fires. Tune all four values in `.env`.

## Connecting a real ESP32 / Arduino later
Because the broker is the public HiveMQ instance, no networking changes are
needed. In your Arduino sketch:
- broker host: `broker.hivemq.com`, port `1883`
- publish to: `<TOPIC_PREFIX>/sensors/raw/<node-id>` (same prefix as `.env`)
- payload: the same JSON shape the simulator uses

The bridge/Kafka/Flink/InfluxDB/Grafana chain can't tell simulated data from
real hardware — it's the same topic and the same JSON.

## Stopping / resetting
```bash
docker compose down          # stop
docker compose down -v        # stop and wipe stored data (Kafka/Influx/Grafana volumes)
```

## Notes & knobs
- **Offsets**: Flink starts from `latest` so restarts don't replay history. Switch
  to `earliest` in `wildfire_job.py` if you want catch-up on restart.
- **Parallelism**: the job runs at parallelism 1 (plenty for 3 nodes). Raise
  `env.set_parallelism(...)` and Kafka topic partitions together to scale.
- **PyFlink version**: pinned to Flink 1.18.1 with `flink-sql-connector-kafka-3.2.0-1.18`.
  Keep these two in lockstep if you upgrade.
- **Security**: everything here uses default/demo credentials and an anonymous
  public broker — fine for development, not for anything real. For production
  you'd move to a private/authenticated broker and rotate the tokens.
