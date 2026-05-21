# Real-time Banking Fraud Detection — Spark Streaming & Kafka

> TP5 — Collective Project · SID45 Big Data Processing · ESP Nouakchott · 2025–2026

An end-to-end platform that ingests a high-throughput stream of banking
transactions, computes real-time per-user statistics for fraud detection, and
displays them on a live dashboard.

---

## 1. Problem

Bank X cannot analyse its transaction flow in real time (up to **1,000 tx/s**
at peak). Fraud detection is reactive instead of proactive. This project
delivers a scalable, fault-tolerant pipeline that turns the raw transaction
stream into actionable per-user fraud-detection metrics.

## 2. Architecture

```
 ┌──────────────┐   JSON over    ┌──────────────┐    consume    ┌──────────────────┐
 │  Transaction  │ ────────────▶ │    Kafka      │ ───────────▶ │   Spark cluster   │
 │   Generator   │   Kafka topic │   (broker)    │   (stream)    │  1 master         │
 │  (container)  │               │               │               │  + 3 workers      │
 └──────────────┘               └──────┬───────┘               └────────┬─────────┘
                                        │                                 ▲
                                  ┌─────▼──────┐          submits the      │
                                  │  Kafka UI  │          streaming job    │
                                  │ monitoring │                           │
                                  └────────────┘                ┌──────────┴──────────┐
                                                                  │   Jupyter container  │
                                  ┌────────────────────┐          │  - runs processor.py │
                                  │  Shared volume      │ ◀────────│  - hosts dashboard   │
                                  │  ./data (Parquet)   │  write   │    notebook          │
                                  └─────────┬──────────┘  / read   └──────────────────────┘
                                            └────────────────────────────▲
```

**Data flow:** the generator simulates 300,000 individuals(change it in .env ) and pushes JSON
transactions to Kafka → the **Spark Streaming processor** (`processor.py`, run
inside the Jupyter container and submitted to the cluster) consumes the stream,
stores every transaction in a Parquet ledger and recomputes all
fraud-detection metrics every few seconds → results land in the shared
`./data` volume → the dashboard notebook reads them and refreshes
automatically.

See [`docs/architecture.md`](docs/architecture.md) for detailed diagrams.

## 3. The 6 services

This project's architecture is exactly the 6 services required by the project
statement (Section 3.2) — defined in `docker-compose.yml`:

| # | Service | Image | URL |
|---|---------|-------|-----|
| 1 | `generator` | `spark-lab-jupyter` | — |
| 2 | `kafka` | `apache/kafka:latest` | — |
| 3 | `kafka-ui` | `provectuslabs/kafka-ui:latest` | http://localhost:8080 |
| 4 | `spark-master` | `sidi-spark:3.5.3-py311` | http://localhost:8081 |
| 5 | `spark-worker-1/2/3` | `sidi-spark:3.5.3-py311` | http://localhost:8082-8084 |
| 6 | `jupyter` | `spark-lab-jupyter` | http://localhost:8888 |

> **The Spark Streaming processor is not a 7th container.** It is application
> code (`processor/processor.py`) that runs **inside the Jupyter container**
> (the project's Python/Spark environment) and is submitted to the Spark
> cluster — this is exactly how the project statement frames it ("Your Spark
> application").

**Reused images.** The Compose file reuses the Docker images already present
on the machine (`apache/kafka`, `provectuslabs/kafka-ui`, the locally built
`sidi-spark:3.5.3-py311` Spark image and the `spark-lab-jupyter` notebook
image) — nothing heavy is rebuilt. The generator uses the Jupyter image
because it already ships `numpy` and `confluent-kafka`, so no runtime
`pip install` is needed.

**Isolation.** The Compose project is named `fraud-detection` and uses its own
network, its own namespaced Kafka volume and a fresh local `./data` folder, so
it never collides with any other project on the machine.

## 4. Computed metrics

For **each identifiable user**, over the **3h / 7d / 3w / 3m** sliding windows
**and** since account creation (lifetime):

- Average amount sent / received
- Number of transactions sent / received
- Distinct receivers / distinct senders (network analysis)
- Lifetime totals and hourly / daily / weekly / monthly averages

## 5. Simulation design choices

The simulation follows Section 4 of the project statement. Key choices:

- **Population:** N = 100,000 Bank X clients, M = 200,000 external users
  (banks A and B, split equally). Configurable in `.env`.
- **Monthly income** `I`: power law `P(I) ∝ 1/I²` on `[1000, 1,000,000]` MRU.
- **Spending per transaction** `S`: uniform on `[I/1000, I/100]`.
- **Initial balance** `B`: uniform on `[0, 3·I]` (0–3 months of income).
- **Transaction frequency** `f = I/S` transactions/month.
- **Per-second probability** `p = f / (30·24·3600)`, rescaled by a global
  multiplier so the stream hits a configurable `TARGET_TPS` (default 300,
  raisable to 1000+ for peak-hour demos).
- **Transaction amount** `A ~ U[S−2σ, S+2σ]` with `σ = S/2`, capped by the
  sender's balance (insufficient-funds transactions are dropped).
- **Backfill:** at startup the generator emits 80,000 synthetic historical
  transactions spread over 90 days so the long windows are meaningful
  immediately.
- **Anomaly highlighting:** the dashboard flags users whose 3h average amount
  exceeds 3× their lifetime average — a simple, explainable fraud heuristic.

Full reasoning is in [`docs/design_decisions.md`](docs/design_decisions.md).

## 6. Setup (quick)

Requirements: **Docker** + **Docker Compose**, the existing images listed
above, and ~6–7 GB free RAM.

```bash
# 1. let the containers write the shared analytics folder
mkdir -p data && chmod 777 data

# 2. start the 6 services (reuses your local images, no heavy build)
docker compose up -d

# 3. open Jupyter -> http://localhost:8888  (token: bigdata)
#    - open a Terminal, run:  python processor/processor.py   (leave it running)
#    - open dashboard.ipynb and Run All Cells
```

Full step-by-step instructions with execution traces are in
[`getting_started.md`](getting_started.md).

## 7. Project structure

```
.
├── README.md                 project overview (this file)
├── getting_started.md        step-by-step usage guide with execution traces
├── docker-compose.yml        the 6-service infrastructure (reuses your images)
├── .env                      all tunable parameters
├── generator/
│   ├── generator.py          SERVICE 1 - transaction generator
│   └── requirements.txt
├── processor/
│   └── requirements.txt
├── jupyter/
│   ├── dashboard.ipynb        SERVICE 6 - live dashboard notebook
│   └── requirements.txt
├── data/                     shared volume (Parquet output, created at runtime)
└── docs/
    ├── DOCUMENTATION.md       complete internal documentation
    ├── architecture.md        diagrams and data flow
    ├── design_decisions.md    design rationale and trade-offs
    ├── multi-laptop.md        running the stack across several laptops on a LAN
    └── presentation.pptx      
```

## 8. Stopping

```bash
docker compose down          # stop and remove the 6 containers
docker compose down -v       # also remove the namespaced Kafka volume
```
