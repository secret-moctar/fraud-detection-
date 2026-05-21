# Getting Started — Step-by-Step Usage Guide

This guide walks through running the platform from scratch, with the
**expected output (execution traces)** at each step.

---

## 0. Prerequisites

- Docker Engine 24+ and the Docker Compose plugin
- ~6–7 GB of free RAM (Kafka + Spark cluster of 1 master + 3 workers + Jupyter)
- The following images already present locally (reused by this project):

```bash
$ docker images | grep -E 'kafka|sidi-spark|jupyter'
apache/kafka                   latest        ...
provectuslabs/kafka-ui         latest        ...
sidi-spark                     3.5.3-py311   ...
spark-lab-jupyter              latest        ...
```

> If `spark-lab-jupyter` or `sidi-spark:3.5.3-py311` are missing, build them
> once from your existing Spark/Jupyter lab folder, then come back here.

---

## 1. Prepare the shared data folder

The Spark and Jupyter containers share a `./data` folder for the analytics
output. Create it and make it writable:

```bash
$ cd project_big_data
$ mkdir -p data && chmod 777 data
```

---

## 2. (Optional) Adjust the configuration

All tunable parameters live in `.env`. Defaults are sensible for a laptop
demo. To simulate a heavier peak-hour load, raise the throughput:

```bash
TARGET_TPS=1000
```

---

## 3. Start the infrastructure

This project **reuses your existing images**, so there is no heavy build —
`docker compose up` just creates the containers.

```bash
$ docker compose up -d
```

Expected output:

```
[+] Running 8/8
 ✔ Network fraud-detection_default       Created
 ✔ Volume  fraud-detection_kafka_data    Created
 ✔ Container fraud-detection-kafka-1          Healthy
 ✔ Container fraud-detection-kafka-ui-1       Started
 ✔ Container fraud-detection-spark-master-1   Started
 ✔ Container fraud-detection-spark-worker-1-1 Started
 ✔ Container fraud-detection-spark-worker-2-1 Started
 ✔ Container fraud-detection-spark-worker-3-1 Started
 ✔ Container fraud-detection-generator-1      Started
 ✔ Container fraud-detection-jupyter-1        Started
```

> The containers are named `fraud-detection-*` and live on their own network
> and volume, so they do **not** collide with your other project's containers.

Confirm everything is up:

```bash
$ docker compose ps
SERVICE          STATUS                    PORTS
generator        Up
jupyter          Up                        127.0.0.1:8888->8888/tcp
kafka            Up (healthy)              127.0.0.1:9092->9092/tcp, 9094
kafka-ui         Up                        127.0.0.1:8080->8080/tcp
spark-master     Up                        127.0.0.1:7077, 8081->8080, 4040
spark-worker-1   Up                        127.0.0.1:8082->8081/tcp
spark-worker-2   Up                        127.0.0.1:8083->8081/tcp
spark-worker-3   Up                        127.0.0.1:8084->8081/tcp
```

---

### What auto-starts and what does NOT

When you run `docker compose up -d`, these **start by themselves** because each
container has a `command` (or default entrypoint) that launches its long-lived
process:

| Container | Auto-starts | Why |
|-----------|-------------|-----|
| `kafka` | Kafka broker daemon | Image's default entrypoint |
| `kafka-ui` | Web UI on :8080 | Image's default entrypoint |
| `spark-master` | Spark master daemon | `command:` runs `spark-class org.apache.spark.deploy.master.Master` |
| `spark-worker-1/2/3` | Spark worker daemon | `command:` runs `spark-class … Worker spark://spark-master:7077` |
| `generator` | `python generator.py` → starts streaming to Kafka **immediately** | We override the entrypoint to run the script |
| `jupyter` | Jupyter Lab on :8888 | Image's default entrypoint |

**The only thing that does NOT auto-start is the Spark Streaming processor**
(`processor/processor.py`). That is intentional — the processor is the project's
*Spark application*, not a service of its own (the project statement defines
exactly 6 services and does not include a processor container). You start it
manually from a Jupyter terminal in Step 6 below, which lets you (1) see the
batch-by-batch logs live, and (2) stop / restart it without touching the
infrastructure.

## 4. Verify the transaction generator

```bash
$ docker compose logs -f generator
```

Expected trace (no pip install at startup — the Jupyter image already ships
the libraries the generator needs):

```
[generator] starting transaction generator
[generator] population: N(bank_X)=100000, M(external)=200000, total=300000
[generator] natural rate=34.7 tx/s, target=300 tx/s, multiplier=8.65
[generator] Kafka producer ready (bootstrap=kafka:9092)
[generator] sending 80000 historical transactions over the last 90 days...
[generator]   backfill progress: 20000/80000
[generator]   backfill progress: 40000/80000
[generator]   backfill progress: 60000/80000
[generator]   backfill progress: 80000/80000
[generator] backfill complete
[generator] entering real-time loop (Ctrl+C to stop)
[generator] t=19:24:37 produced=285 tx  total=285
[generator] t=19:24:38 produced=297 tx  total=582
```

---

## 5. Monitor Kafka

Open **http://localhost:8080** (Kafka UI).

- Cluster `sid45` → **Topics** → `transactions`
- The partition offsets should be growing; click **Messages** to inspect raw
  JSON, e.g.:

```json
{
  "msg_entity": "bank_X",
  "app_type": "mobile_app",
  "send_entity": "bank_X",
  "receive_entity": "bank_A",
  "send_id": "user_0012345",
  "receive_id": "user_0167890",
  "amount": 1523.40,
  "date": "2026-05-19T14:32:15Z",
  "tx_type": "transfer",
  "tx_id": "7c9e6a1d-..."
}
```

---

## 6. Start the Spark Streaming processor

The processor is a Spark application you launch
from inside the Jupyter container, which already has the Spark master URL and
the Kafka connector configured.

1. Open **http://localhost:8888** and enter the token **`bigdata`**
   (or open `http://localhost:8888/?token=bigdata`).
2. In Jupyter Lab: **File → New → Terminal**.
3. In that terminal, run:

```bash
$ python processor/processor.py
```


> The first few batches drain the 80,000-transaction(depend on .env) backfill; later batches
> reflect the live `TARGET_TPS` stream. **Leave this terminal running** — it is
> the processor. Open http://localhost:8081 to see the application `RUNNING`
> on the Spark master with 3 workers registered.

Check that output files appear:

```bash
$ ls data/output
recent_tx  user_metrics
$ ls data/tx_store
dt=2026-02-20  dt=2026-03-11  ...  dt=2026-05-19
```

---

## 7. Open the live dashboard

1. Back in Jupyter Lab (browser), open `dashboard.ipynb`.
2. Menu → **Run → Run All Cells**.
3. The last cell starts the auto-refreshing dashboard (every 5 s).

Expected display:

```
==============================================================================
   REAL-TIME BANKING FRAUD DETECTION  -  Bank X monitoring committee
   refreshed at 2026-05-19 14:33:05
==============================================================================

Activity in the last 10s : 2981 transactions   (~298.1 tx/s)
Distinct users currently tracked by Spark : 51840

--- AVERAGE AMOUNTS  (sent / received)  -  last 20 users ---
 user           bank    avg_amount_sent_3h  avg_amount_recv_3h  ...
 ...
--- TRANSACTION COUNTS & DISTINCT NETWORK  -  last 20 users ---
 ...
--- SINCE ACCOUNT CREATION (lifetime)  -  last 20 users ---
 ...

[two charts: transactions/second over last 60s, top users by 3h tx count]

[!] 2 user(s) flagged with anomalous spending spikes (red rows above).
```

Rows in **red** are users whose recent (3h) average amount is more than 3×
their lifetime average — a candidate fraud signal. Stop the dashboard with the
kernel **interrupt / stop** button.

---

## 8. Common operations

```bash
# follow all logs
docker compose logs -f

# change throughput, then apply (edit TARGET_TPS in .env first)
docker compose up -d generator

# inspect the Parquet output from the host
python3 -c "import pandas as pd; print(pd.read_parquet('data/output/user_metrics').head())"
```

---

## 9. Shut down

```bash
docker compose down        # stop and remove the 6 containers (keeps ./data)
docker compose down -v     # also remove the namespaced Kafka volume
rm -rf data/tx_store data/output data/checkpoints   # clear analytics output
```

---

## 10. Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `processor.py` fails to find PySpark | Run it **inside the Jupyter container terminal**, not on the host. |
| Processor first run is slow to start | It downloads the Kafka connector JAR once (via `--packages`); needs internet the first time. |
| Dashboard says "Waiting for data..." | The processor has not produced its first batch yet — wait ~1 min after starting `processor.py`. Also check `docker exec fraud-detection-jupyter-1 tail /tmp/processor.log` for errors. |
| **`StreamingQueryException: ... offset was changed ... data may have been missed`** | You ran `docker compose down -v` (Kafka wiped) without clearing `./data/checkpoints/`. The processor already includes `failOnDataLoss=false` to tolerate this, but the cleanest fix is to ALWAYS pair the two: `docker compose down -v && rm -rf data/tx_store data/output data/checkpoints` before the next `up -d`. |
| `Spark master UI :8081 shows 3 workers but "Running Applications" is empty` | This is **expected when no Spark application is running**. The workers are *waiting for work*. They become busy only once `python processor/processor.py` is launched. |
| `Permission denied` on `data/` | Run `chmod 777 data` before `docker compose up`. |
| Spark workers not registered | Open http://localhost:8081; restart with `docker compose restart spark-worker-1 spark-worker-2 spark-worker-3`. |
| Not enough RAM for 3 workers | Comment out `spark-worker-2` / `spark-worker-3` in `docker-compose.yml` (1 worker still works). |
| Port already in use | Another project is still running — `docker compose ls` and stop it, or change the host port in `docker-compose.yml`. |
| Want to distribute across LAN laptops | See [`docs/multi-laptop.md`](docs/multi-laptop.md). |
