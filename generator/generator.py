"""
Transaction Generator
=====================
Simulates a population of bank customers and produces a continuous stream of
banking transactions to a Kafka topic, following the specification in the TP5
project statement (Section 4 - Data Simulation Specification).

The generator does three things:
  1. Builds a synthetic population of N + M individuals with realistic
     financial attributes (income, spending, balance, transaction frequency).
  2. (Optional) Sends a one-off "backfill" of historical transactions so that
     the long sliding windows (7 days / 3 weeks / 3 months) already contain
     data when the demo starts.
  3. Runs an infinite loop that, every wall-clock second, decides which
     individuals transact, builds JSON messages and pushes them to Kafka.

All tuning is done through environment variables (see the .env file).
"""

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np
from confluent_kafka import Producer, KafkaException

# --------------------------------------------------------------------------
# Configuration (read from environment - see .env / docker-compose.yml)
# --------------------------------------------------------------------------
# KAFKA_BOOTSTRAP_SERVERS is the variable name used across this stack; we also
# accept KAFKA_BOOTSTRAP as a fallback.
KAFKA_BOOTSTRAP = (os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
                   or os.environ.get("KAFKA_BOOTSTRAP")
                   or "kafka:9092")
KAFKA_TOPIC     = os.environ.get("KAFKA_TOPIC", "transactions")

POP_BANK_X      = int(os.environ.get("POP_BANK_X", "100000"))   # N - Bank X clients
POP_EXTERNAL    = int(os.environ.get("POP_EXTERNAL", "200000")) # M - banks A and B
TARGET_TPS      = float(os.environ.get("TARGET_TPS", "300"))    # desired transactions/second

BACKFILL_TX     = int(os.environ.get("BACKFILL_TX", "80000"))   # historical tx sent at startup
BACKFILL_DAYS   = int(os.environ.get("BACKFILL_DAYS", "90"))    # spread of the historical tx

SEED            = int(os.environ.get("SEED", "42"))

# Fixed value lists used to fill descriptive JSON fields.
APP_TYPES = ["mobile_app", "web_app", "atm", "pos", "ussd"]
TX_TYPES  = ["transfer", "payment", "withdrawal", "deposit"]

# Seconds in one month - used to convert "transactions per month" into a
# per-second probability, exactly as in the project statement.
SECONDS_PER_MONTH = 30 * 24 * 3600

# --------------------------------------------------------------------------
# Graceful shutdown - lets `docker compose stop` end the loop cleanly.
# --------------------------------------------------------------------------
_running = True


def _stop(signum, frame):
    global _running
    print("[generator] shutdown signal received, stopping...", flush=True)
    _running = False


signal.signal(signal.SIGINT, _stop)
signal.signal(signal.SIGTERM, _stop)


# --------------------------------------------------------------------------
# 1. Population initialisation
# --------------------------------------------------------------------------
def init_population(n_x, n_ext, seed):
    """
    Create the N + M individuals and their attributes.

    Returns numpy arrays so that the per-second simulation step stays fully
    vectorised (fast even for 300,000 individuals).

      income   I_i : monthly fixed income, power-law distributed
      spend    S_i : average spending per transaction, uniform on [I/1000, I/100]
      balance  B_i : account balance, uniform on [0, 3*I]
      freq     f_i : transactions per month = I / S
      bank         : "bank_X" | "bank_A" | "bank_B"
    """
    rng = np.random.default_rng(seed)
    total = n_x + n_ext

    # --- Monthly income: power law  P(I) ~ C / I^2  on [1000, 1000000] -----
    # We sample by inverse-transform. The CDF of C/I^2 is
    #   F(I) = C * (1/a - 1/I)   with  C = 1 / (1/a - 1/b)
    # Inverting F(I) = u  gives the closed form below.
    a, b = 1_000.0, 1_000_000.0
    u = rng.random(total)
    income = 1.0 / (1.0 / a - u * (1.0 / a - 1.0 / b))

    # --- Average spending per transaction: U[I/1000, I/100] ----------------
    spend = rng.uniform(income / 1000.0, income / 100.0)

    # --- Initial balance: U[0, 3*I]  (0 to 3 months of income) -------------
    balance = rng.uniform(0.0, 3.0 * income)

    # --- Transaction frequency f = I / S  (transactions per month) ---------
    freq = income / spend

    # --- Bank assignment ---------------------------------------------------
    # First N individuals belong to Bank X; the remaining M are split
    # equally between bank_A and bank_B.
    bank = np.empty(total, dtype=object)
    bank[:n_x] = "bank_X"
    half = (total - n_x) // 2
    bank[n_x:n_x + half] = "bank_A"
    bank[n_x + half:] = "bank_B"

    return rng, income, spend, balance, freq, bank


def uid(i):
    """Stable string identifier for individual index i."""
    return f"user_{int(i):07d}"


# --------------------------------------------------------------------------
# 2. Kafka producer
# --------------------------------------------------------------------------
def connect_producer():
    """Create a confluent-kafka Producer (it connects lazily on first send)."""
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 20,          # small batching window -> higher throughput
        "acks": "1",              # leader ack is enough for a demo
        "queue.buffering.max.messages": 200000,
    })
    print(f"[generator] Kafka producer ready (bootstrap={KAFKA_BOOTSTRAP})",
          flush=True)
    return producer


def _produce(producer, value):
    """Send one JSON message, draining the local queue if it fills up."""
    payload = json.dumps(value).encode("utf-8")
    while True:
        try:
            producer.produce(KAFKA_TOPIC, payload)
            return
        except BufferError:
            # Local queue is full: give librdkafka time to flush callbacks.
            producer.poll(0.1)


# --------------------------------------------------------------------------
# 3. Message construction
# --------------------------------------------------------------------------
def build_message(s_idx, r_idx, amount, ts_iso, bank):
    """Build one transaction message exactly in the required JSON format."""
    return {
        "msg_entity":     bank[s_idx],          # bank that originated the message
        "app_type":       random.choice(APP_TYPES),
        "send_entity":    bank[s_idx],          # sender's bank
        "receive_entity": bank[r_idx],          # receiver's bank
        "send_id":        uid(s_idx),
        "receive_id":     uid(r_idx),
        "amount":         round(float(amount), 2),
        "date":           ts_iso,               # ISO 8601 timestamp
        "tx_type":        random.choice(TX_TYPES),
        "tx_id":          str(uuid.uuid4()),
    }


def pick_receivers(rng, senders, total):
    """
    Pick a random receiver for every sender, making sure nobody sends money
    to themselves. We re-draw only the colliding entries (vectorised).
    """
    receivers = rng.integers(0, total, size=len(senders))
    for _ in range(5):
        collision = receivers == senders
        if not collision.any():
            break
        receivers[collision] = rng.integers(0, total, size=int(collision.sum()))
    return receivers


# --------------------------------------------------------------------------
# 4. Historical backfill
# --------------------------------------------------------------------------
def send_backfill(producer, rng, spend, bank, total):
    """
    Send `BACKFILL_TX` synthetic historical transactions whose timestamps are
    spread over the last `BACKFILL_DAYS` days. This is what makes the long
    sliding windows (7d / 3w / 3m) show meaningful, *different* values right
    away instead of being empty at the start of a short demo.

    Note: backfill transactions do NOT enforce the balance constraint - they
    are purely synthetic history used to warm up the analytics.
    """
    if BACKFILL_TX <= 0:
        print("[generator] backfill disabled", flush=True)
        return

    print(f"[generator] sending {BACKFILL_TX} historical transactions "
          f"over the last {BACKFILL_DAYS} days...", flush=True)

    now = datetime.now(timezone.utc)
    senders   = rng.integers(0, total, size=BACKFILL_TX)
    receivers = pick_receivers(rng, senders, total)
    amounts   = rng.uniform(0.0, 2.0 * spend[senders])
    # Random age (in seconds) for each transaction.
    ages = rng.integers(0, BACKFILL_DAYS * 24 * 3600, size=BACKFILL_TX)

    for k in range(BACKFILL_TX):
        ts = now - timedelta(seconds=int(ages[k]))
        msg = build_message(senders[k], receivers[k], amounts[k],
                            ts.strftime("%Y-%m-%dT%H:%M:%SZ"), bank)
        _produce(producer, msg)
        if (k + 1) % 20000 == 0:
            producer.flush()
            print(f"[generator]   backfill progress: {k + 1}/{BACKFILL_TX}", flush=True)

    producer.flush()
    print("[generator] backfill complete", flush=True)


# --------------------------------------------------------------------------
# 5. Main real-time simulation loop
# --------------------------------------------------------------------------
def main():
    print("[generator] starting transaction generator", flush=True)
    print(f"[generator] population: N(bank_X)={POP_BANK_X}, "
          f"M(external)={POP_EXTERNAL}, total={POP_BANK_X + POP_EXTERNAL}", flush=True)

    rng, income, spend, balance, freq, bank = init_population(
        POP_BANK_X, POP_EXTERNAL, SEED)
    total = POP_BANK_X + POP_EXTERNAL

    # --- Per-second transaction probability --------------------------------
    # p_i = f_i / (30 * 24 * 3600)   as defined in the statement.
    p_base = freq / SECONDS_PER_MONTH

    # The raw probabilities give a fixed natural throughput. We rescale them
    # with a single global multiplier so the stream matches TARGET_TPS, which
    # lets us demo anything from a light load up to 1000+ tx/s.
    natural_tps = float(p_base.sum())
    multiplier = TARGET_TPS / natural_tps if natural_tps > 0 else 1.0
    p = np.clip(p_base * multiplier, 0.0, 1.0)
    print(f"[generator] natural rate={natural_tps:.1f} tx/s, "
          f"target={TARGET_TPS:.0f} tx/s, multiplier={multiplier:.2f}", flush=True)

    producer = connect_producer()
    send_backfill(producer, rng, spend, bank, total)

    print("[generator] entering real-time loop (Ctrl+C to stop)", flush=True)
    total_sent = 0
    while _running:
        loop_start = time.time()

        # --- Decide who transacts this second -----------------------------
        draws = rng.random(total)
        senders = np.where(draws < p)[0]

        produced = 0
        if len(senders) > 0:
            receivers = pick_receivers(rng, senders, total)

            # Transaction amount: A ~ U[S - 2*sigma, S + 2*sigma] with
            # sigma = S/2, which simplifies to U[0, 2*S].
            amounts = rng.uniform(0.0, 2.0 * spend[senders])

            # Balance constraint: a transaction is only created if the sender
            # can afford it (A <= B_sender).
            affordable = amounts <= balance[senders]
            senders   = senders[affordable]
            receivers = receivers[affordable]
            amounts   = amounts[affordable]

            # Update balances: sender loses, receiver gains.
            np.subtract.at(balance, senders, amounts)
            np.add.at(balance, receivers, amounts)

            ts_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            for k in range(len(senders)):
                msg = build_message(senders[k], receivers[k],
                                    amounts[k], ts_iso, bank)
                _produce(producer, msg)
                produced += 1

        producer.flush()
        total_sent += produced
        print(f"[generator] t={datetime.now().strftime('%H:%M:%S')} "
              f"produced={produced} tx  total={total_sent}", flush=True)

        # --- Keep a steady 1-second cadence -------------------------------
        elapsed = time.time() - loop_start
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

    producer.flush()
    print(f"[generator] stopped. total transactions sent: {total_sent}", flush=True)


if __name__ == "__main__":
    main()
