# Running the project on multiple laptops (LAN)

The single-laptop stack defined in `docker-compose.yml` already implements the
project specification end to end. To run it **distributed across several
laptops on the same network**, only the network configuration needs to change.
The application code is untouched.

This guide assumes 2 to 4 laptops on the same LAN, all able to ping each
other.

---

## 1. The plan

Pick **one** laptop to be the **main host**. That laptop runs the full
docker-compose stack from this repo. Every other laptop runs a **single extra
Spark worker** container that connects to the main laptop's Spark master.

```
                ╔══════════════════ Main laptop (LAN 192.168.1.42) ═══════════════════╗
                ║                                                                       ║
                ║  kafka  kafka-ui  spark-master  spark-worker-1/2/3  jupyter  generator ║
                ║                          ▲             ▲                                ║
                ╚══════════════════════════│═════════════│════════════════════════════════╝
                                           │ :7077       │ :9094 (Kafka EXTERNAL)
                                           │             │
              ┌────────── Laptop B ────────┼──┐   ┌──────┼──── Laptop C ─────────┐
              │   extra-spark-worker  ─────┘  │   │  ────┘   extra-spark-worker  │
              └───────────────────────────────┘   └──────────────────────────────┘
```

This is enough to demonstrate true horizontal scaling: tasks scheduled by the
master are distributed across workers running on *physically different
machines*.

---

## 2. Configuration changes on the MAIN laptop

### a) Find the LAN IP

```bash
$ hostname -I | awk '{print $1}'
192.168.1.42        # example - use your actual value
```

### b) Tell Kafka to advertise that IP

Edit `.env`:

```
HOST_IP=192.168.1.42
```

That single value is interpolated into the Compose file as Kafka's
`KAFKA_ADVERTISED_LISTENERS=...EXTERNAL://${HOST_IP}:9094`, so any client on
the LAN can connect to `192.168.1.42:9094` and Kafka will respond with the
same address (instead of `localhost`, which only worked from the main host).

### c) Expose the ports to the LAN (one quick edit)

In `docker-compose.yml`, change the bound interface from `127.0.0.1` to
`0.0.0.0` for the three ports remote machines will reach:

```yaml
  kafka:
    ports:
      - "0.0.0.0:9094:9094"     # Kafka EXTERNAL listener (was 127.0.0.1)

  spark-master:
    ports:
      - "0.0.0.0:7077:7077"     # Spark master RPC      (was 127.0.0.1)
      - "0.0.0.0:8081:8080"     # Spark master web UI   (was 127.0.0.1)
```

Keep `kafka-ui` (`:8080`) and `jupyter` (`:8888`) on `127.0.0.1` — only the
main user opens those.

> If the laptop has a firewall (`ufw`, Windows Defender, etc.), allow inbound
> TCP on **7077** (Spark master) and **9094** (Kafka EXTERNAL).

### d) Bring the stack up

```bash
docker compose up -d
```

---

## 3. On EVERY other laptop (worker-only)

One Docker command — no Compose, no source code, no Python:

```bash
docker run -d \
    --name fraud-extra-worker \
    --network host \
    sid-spark:3.5.3 \
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
    
        spark://192.168.1.42:7077
```

What this does:

- `apache/spark:3.5.3` is the official Apache Spark image on Docker Hub
  (Bitnami's `bitnami/spark` repository was archived in 2025 and is no longer
  pullable). The 3.5.3 tag matches our cluster version exactly.
  Pull it once with `docker pull apache/spark:3.5.3` (~600 MB).
- `--network host` removes Docker's NAT so the worker can both reach the
  master and accept executor traffic on the LAN.
- The last line is exactly the same command our local workers run inside the
  main `docker-compose.yml` — only the master URL changes.

After ~10 seconds, open the master UI **from the main laptop**
http://localhost:8081 — you should see the extra worker(s) listed alongside
the three local ones.

To remove a remote worker: `docker rm -f fraud-extra-worker` on that laptop.

---

> If the apache/spark image cannot reach the master (you see no new line in
> the master UI within ~15 s), `docker logs fraud-extra-worker` usually says
> "Cannot connect to spark://...". Confirm: (a) the worker laptop can `nc
> -zv <MAIN_IP> 7077`; (b) the main laptop's firewall lets in port 7077;
> (c) the port binding in `docker-compose.yml` is on `0.0.0.0`, not
> `127.0.0.1`.

## 4. Verifying the distribution

Open **http://localhost:8081** (main laptop). In the "Workers" table you
should see one row per worker container, including the remote ones — each
row shows its host IP and its declared cores / memory.

Start the processor as usual (`python processor/processor.py` in a Jupyter
terminal). In the master UI, click on the running "FraudDetectionProcessor"
application → "Executors" — the executor list shows tasks running on every
worker, including the remote ones.

> **Tip:** the simplest way to *prove* multi-laptop execution to the audience
> is to remove the local workers (`docker compose stop spark-worker-2
> spark-worker-3`) and rely on the remote ones for a moment. The processor
> keeps running, with all stages executing on the other laptops.

---

## 5. Optional: also offload the generator

If you want the *producer* to also run from another laptop, you need our
generator image (`spark-lab-jupyter`) on that machine. The simplest way is to
copy the image from the main laptop over the LAN (no rebuild, no internet):

```bash
# on the MAIN laptop:
docker save spark-lab-jupyter | ssh user@<other-laptop> 'docker load'
```

Then on that laptop, with the project's `generator/` folder copied locally:

```bash
docker run -d \
    --name fraud-extra-generator \
    --network host \
    -v $PWD/generator:/opt/generator:ro \
    -e KAFKA_BOOTSTRAP_SERVERS=192.168.1.42:9094 \
    -e KAFKA_TOPIC=transactions \
    -e POP_BANK_X=50000 -e POP_EXTERNAL=100000 \
    -e TARGET_TPS=300 -e BACKFILL_TX=0 \
    --entrypoint /opt/conda/bin/python \
    spark-lab-jupyter \
    /opt/generator/generator.py
```

(`BACKFILL_TX=0` so the second generator doesn't duplicate the historical
backfill produced by the main one.)

---

## 6. Summary — what to change

| You only modify | Where | Why |
|-----------------|-------|-----|
| `HOST_IP` | `.env` | Kafka advertises a LAN-reachable address |
| Three port bindings | `docker-compose.yml` (kafka, spark-master) | Listen on `0.0.0.0` instead of `127.0.0.1` |
| One `docker run` | each remote laptop | Register an extra Spark worker |

Everything else — the generator code, the processor code, the dashboard, the
metric definitions — works **identically** in single-laptop and multi-laptop
mode.
