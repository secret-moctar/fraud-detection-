import os
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

KAFKA_BOOTSTRAP  = (os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
                    or os.environ.get("KAFKA_BOOTSTRAP")
                    or "kafka:9092")
KAFKA_TOPIC      = os.environ.get("KAFKA_TOPIC", "transactions")
DATA_DIR         = os.environ.get("DATA_DIR", "/workspace/data")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "5 seconds")

TX_STORE   = f"{DATA_DIR}/tx_store"
OUT_DIR    = f"{DATA_DIR}/output"
CHECKPOINT = f"{DATA_DIR}/checkpoints/processor"


WINDOWS = [
    ("3h", 3 * 3600),
    ("7d", 7 * 86400),
    ("3w", 21 * 86400),
    ("3m", 90 * 86400),
]

#json

MESSAGE_SCHEMA = T.StructType([
    T.StructField("msg_entity",     T.StringType()),
    T.StructField("app_type",       T.StringType()),
    T.StructField("send_entity",    T.StringType()),
    T.StructField("receive_entity", T.StringType()),
    T.StructField("send_id",        T.StringType()),
    T.StructField("receive_id",     T.StringType()),
    T.StructField("amount",         T.DoubleType()),
    T.StructField("date",           T.StringType()),
    T.StructField("tx_type",        T.StringType()),
    T.StructField("tx_id",          T.StringType()),
])


# --------------------------------------------------------------------------
# Metric computation
# --------------------------------------------------------------------------
def compute_side(ledger, role, now):
    """
    Returns one row per user with:
      count_<role>_<window>          number of transactions
      avg_amount_<role>_<window>     average amount
      distinct_<role>_<window>       distinct counterparties
    for every window, plus the lifetime versions and total_<role> / first_seen.
    """
    # --- Lifetime ("since account creation") = the whole ledger ------------
    result = ledger.groupBy("user").agg(
        F.count("*").alias(f"count_{role}_life"),
        F.avg("amount").alias(f"avg_amount_{role}_life"),
        F.sum("amount").alias(f"total_{role}"),
        F.countDistinct("counterparty").alias(f"distinct_{role}_life"),
        F.min("ts").alias(f"first_seen_{role}"),
    )

    # --- One windowed aggregation per sliding window -----------------------
    for label, seconds in WINDOWS:
        cutoff = now - timedelta(seconds=seconds)
        windowed = (
            ledger.filter(F.col("ts") >= F.lit(cutoff))
            .groupBy("user")
            .agg(
                F.count("*").alias(f"count_{role}_{label}"),
                F.avg("amount").alias(f"avg_amount_{role}_{label}"),
                F.countDistinct("counterparty").alias(f"distinct_{role}_{label}"),
            )
        )
        result = result.join(windowed, "user", "left")

    return result


def process_batch(batch_df, batch_id):
    """Called once per micro-batch by Structured Streaming."""
    count = batch_df.count()
    ts_now = datetime.now(timezone.utc).strftime("%H:%M:%S")
    if count == 0:
        print(f"[processor] {ts_now} batch {batch_id}: empty, skipping", flush=True)
        return

    print(f"[processor] {ts_now} batch {batch_id}: {count} new transactions",
          flush=True)
    spark = batch_df.sparkSession

    # --- 1. Append the new transactions to the ledger ----------------------
    clean = (
        batch_df
        .filter(F.col("tx_id").isNotNull() & F.col("amount").isNotNull())
        .withColumn("ts", F.to_timestamp("date", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("dt", F.date_format("ts", "yyyy-MM-dd"))   
        .select("msg_entity", "app_type", "send_entity", "receive_entity",
                "send_id", "receive_id", "amount", "ts", "tx_type", "tx_id", "dt")
    )
    clean.write.mode("append").partitionBy("dt").parquet(TX_STORE)

    # --- 2. Read the whole ledger back -------------------------------------
    ledger = spark.read.parquet(TX_STORE).cache()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Two views of the same ledger: one keyed by sender, one by receiver.
    sent = ledger.select(
        F.col("send_id").alias("user"),
        F.col("receive_id").alias("counterparty"),
        "amount", "ts")
    recv = ledger.select(
        F.col("receive_id").alias("user"),
        F.col("send_id").alias("counterparty"),
        "amount", "ts")

    metrics_sent = compute_side(sent, "sent", now)
    metrics_recv = compute_side(recv, "recv", now)

    # --- 3. Each user's bank (X / A / B) -----------------------------------
    user_bank = (
        ledger.select(F.col("send_id").alias("user"),
                      F.col("send_entity").alias("bank"))
        .union(ledger.select(F.col("receive_id").alias("user"),
                             F.col("receive_entity").alias("bank")))
        .dropDuplicates(["user"])
    )

    # --- 4. Join everything into one wide per-user table -------------------
    metrics = (
        user_bank
        .join(metrics_sent, "user", "left")
        .join(metrics_recv, "user", "left")
    )

    # Lifetime periodic averages "since account creation":
    # total amount divided by the time elapsed since the user's first tx.
    first_seen = F.least(F.col("first_seen_sent"), F.col("first_seen_recv"))
    elapsed_h = F.greatest(
        (F.lit(now).cast("long") - first_seen.cast("long")) / 3600.0,
        F.lit(1.0))
    metrics = (
        metrics
        .withColumn("elapsed_hours", elapsed_h)
        .withColumn("avg_hourly_sent",  F.coalesce(F.col("total_sent"), F.lit(0.0)) / F.col("elapsed_hours"))
        .withColumn("avg_daily_sent",   F.coalesce(F.col("total_sent"), F.lit(0.0)) / (F.col("elapsed_hours") / 24.0))
        .withColumn("avg_weekly_sent",  F.coalesce(F.col("total_sent"), F.lit(0.0)) / (F.col("elapsed_hours") / 168.0))
        .withColumn("avg_monthly_sent", F.coalesce(F.col("total_sent"), F.lit(0.0)) / (F.col("elapsed_hours") / 720.0))
        .withColumn("avg_hourly_recv",  F.coalesce(F.col("total_recv"), F.lit(0.0)) / F.col("elapsed_hours"))
        .withColumn("avg_daily_recv",   F.coalesce(F.col("total_recv"), F.lit(0.0)) / (F.col("elapsed_hours") / 24.0))
        .withColumn("avg_weekly_recv",  F.coalesce(F.col("total_recv"), F.lit(0.0)) / (F.col("elapsed_hours") / 168.0))
        .withColumn("avg_monthly_recv", F.coalesce(F.col("total_recv"), F.lit(0.0)) / (F.col("elapsed_hours") / 720.0))
    )

    # --- 5. Write outputs for the dashboard --------------------------------
    # coalesce(1) -> a single Parquet file, easy and fast for pandas to read.
    metrics.coalesce(1).write.mode("overwrite").parquet(f"{OUT_DIR}/user_metrics")

    recent_cutoff = now - timedelta(seconds=120)
    (ledger.filter(F.col("ts") >= F.lit(recent_cutoff))
           .select("send_id", "receive_id", "amount", "ts",
                   "send_entity", "receive_entity", "tx_type", "tx_id")
           .coalesce(1)
           .write.mode("overwrite").parquet(f"{OUT_DIR}/recent_tx"))

    n_users = metrics.count()
    ledger.unpersist()
    print(f"[processor] {ts_now} batch {batch_id}: metrics updated for "
          f"{n_users} users", flush=True)


# --------------------------------------------------------------------------
# Streaming entry point
# --------------------------------------------------------------------------
def main():
    spark = (
        SparkSession.builder
        .appName("FraudDetectionProcessor")
        # 8 shuffle partitions is plenty for a single-machine demo cluster.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[processor] reading topic '{KAFKA_TOPIC}' from {KAFKA_BOOTSTRAP}",
          flush=True)

    # Read the raw Kafka stream.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        # "earliest" so we also pick up the historical backfill the generator
        # sends at startup, regardless of which container started first.
        .option("startingOffsets", "earliest")
        # Caps the size of each micro-batch. 20k keeps each batch fast on a
        # modest cluster - the recompute is O(ledger size), so smaller batches
        # actually drain faster overall than one giant batch.
        .option("maxOffsetsPerTrigger", "2000")
        # Tolerate a wiped Kafka topic when the on-disk checkpoint still
        # references old offsets (typical after `docker compose down -v`
        # without clearing ./data/checkpoints). Without this option Spark
        # raises "Partition X's offset was changed ... data may have been
        # missed" and the streaming query terminates.
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("d")
    ).select("d.*")

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)   # offsets -> fault tolerance
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    print("[processor] streaming query started", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()
