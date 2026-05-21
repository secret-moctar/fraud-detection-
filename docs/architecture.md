# Architecture & Data Flow

## 1. Component diagram

```
                          ┌───────────────────────────────────────────────────┐
                          │         Docker Compose project: fraud-detection     │
                          │                                                     │
  ┌─────────────┐         │   ┌──────────────┐        ┌────────────────┐        │
  │  generator   │  JSON   │   │              │        │  spark-master   │        │
  │ (spark-lab-  │ ──────▶ │──▶│    kafka      │        │  (coordinator)  │        │
  │  jupyter)    │ produce │   │   broker      │        └───────┬────────┘        │
  │ 300k people  │         │   │  topic:       │                │ schedules       │
  │ NumPy sim    │         │   │  transactions │        ┌───────▼────────┐        │
  └─────────────┘         │   └──────┬───────┘          │ spark-worker-1 │       │
                          │          │  consume         │ spark-worker-2 │        │
  ┌─────────────┐         │          │                  │ spark-worker-3 │        │
  │  kafka-ui    │ ◀───────│──────────┘                 │other workers   │        │
  │ :8080        │         │                            └───────┬────────┘        │
  └─────────────┘         │                                      │ run tasks       │
                          │   ┌──────────────────────────────────▼─────────────┐  │
  ┌─────────────────┐     │   │              jupyter container                   │  │
  │  your browser    │ ───▶│──▶│  - terminal: `python processor/processor.py`     │  │
  │  :8888           │     │   │      = the Spark Streaming DRIVER                 │  │
  └─────────────────┘     │   │  - dashboard.ipynb  = the live dashboard          │  │
                          │   └───────────────────────┬───────────────────────────┘  │
                          │                            │ write / read                │
                          │   ┌────────────────────────▼──────────────────────────┐ │
                          │   │   ./data  (bind-mounted shared volume)               │ │
                          │   │   tx_store/   output/user_metrics   output/recent_tx │ │
                          │   └──────────────────────────────────────────────────────┘ │
                          └───────────────────────────────────────────────────┘
```

## 2. The 6 services (and why there is no 7th)


1. `generator` — transaction generator
2. `kafka` — Kafka broker
3. `kafka-ui` — Kafka monitoring web UI
4. `spark-master` — Spark cluster coordinator
5. `spark-worker-1/2/3` — 3 Spark workers
6. `jupyter` — the Python/Spark environment + the dashboard

The **Spark Streaming processor** is *not* a service. The statement describes
it as "Your Spark application" — application code submitted to the cluster, not
infrastructure. We therefore keep it as a script (`processor/processor.py`)
that is launched **inside the `jupyter` container** (which already has the
Spark master URL and the Kafka connector wired through `PYSPARK_SUBMIT_ARGS`).
When it runs, the `jupyter` container becomes the Spark **driver** and the work
is distributed across the 3 worker containers.

## 3. End-to-end data flow

| # | Stage | What happens |
|---|-------|--------------|
| 1 | **Generate** | The `generator` simulates 300,000 individuals. Every second it draws who transacts, builds JSON messages and sends them to the Kafka topic `transactions`. |
| 2 | **Buffer** | Kafka stores the messages durably. It decouples the fast producer from the processing layer and absorbs bursts. |
| 3 | **Consume** | `processor.py` (running in the Jupyter container, submitted to the cluster) reads the topic in micro-batches every 5 s. |
| 4 | **Persist** | Each micro-batch is appended to a partitioned Parquet *ledger* (`data/tx_store`, partitioned by date). |
| 5 | **Aggregate** | Spark reads the ledger back and recomputes every required metric with batch SQL: per-window averages/counts, distinct senders/receivers, lifetime stats. |
| 6 | **Publish** | Results are written as Parquet to `data/output/user_metrics` and `data/output/recent_tx`. |
| 7 | **Visualise** | `dashboard.ipynb` (same Jupyter container) reads those Parquet files every 5 s and renders tables + charts with anomaly highlighting. |

## 4. Why a message queue in the middle?

Kafka sits between the generator and Spark so that:

- the **producer never blocks** — it writes to Kafka at full speed regardless
  of how fast Spark consumes;
- **bursts are absorbed** — peak-hour spikes are buffered on disk;
- the design is **fault tolerant** — if the processor stops and is restarted,
  it resumes from its last committed Kafka offset (stored in the checkpoint)
  and loses nothing.

## 5. Storage layout (`./data`, mounted as `/workspace/data`)

```
data/
├── tx_store/                     append-only transaction ledger
│   ├── dt=2026-02-20/ *.parquet
│   └── ...                       (one partition per calendar day)
├── output/
│   ├── user_metrics/  *.parquet  one row per user, ALL fraud-detection metrics
│   └── recent_tx/     *.parquet  last ~2 minutes of raw transactions
└── checkpoints/
    └── processor/                Spark Structured Streaming checkpoint
                                  (Kafka offsets -> exactly-once on restart)
```

This folder is bind-mounted into `spark-master`, the 3 workers and `jupyter`,
all at `/workspace/data`, so every part of the cluster can read and write it.

## 6. The Spark cluster

A **Spark Standalone** cluster: 1 master + 3 workers, as required.

- The **master** schedules work and exposes a web UI (`:8081`).
- Each **worker** offers 2 cores / 1.5 GB → 6 cores total for parallelism.
- The **driver** is the `jupyter` container: `python processor/processor.py`
  submits the streaming job, and the master distributes the tasks across the
  3 workers.

This demonstrates **horizontal scalability** (add more workers → more
throughput) and **fault tolerance** (a worker can die; the master reschedules
its tasks).

## 7. Sliding windows

The processor maintains four sliding windows plus the lifetime view:

```
 now
  │
  ├──── 3h  ────┐  short-term behaviour  (sudden spikes = fraud signal)
  ├──── 7d  ────┤  weekly behaviour
  ├──── 3w  ────┤  monthly trend
  ├──── 3m  ────┤  quarterly baseline
  └──── lifetime  since account creation (whole ledger)
```

Each metric (average amount, count, distinct counterparties) is computed for
every window, for both the *sent* and *received* directions.

## 8. Isolation from other projects on the machine

- The Compose **project name** is `fraud-detection` → its own network and a
  namespaced volume `fraud-detection_kafka_data`.
- **No `container_name:`** is set, so containers are auto-named
  `fraud-detection-<service>-1` and never clash with the (stopped) `kafka`,
  `spark-master`, … containers of the original lab folder.
- Containers still reach each other by **service name** (`kafka`,
  `spark-master`, …) via Compose's automatic network aliases.
