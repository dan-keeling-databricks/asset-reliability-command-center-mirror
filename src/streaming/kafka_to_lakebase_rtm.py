# Databricks notebook source
# MAGIC %md
# MAGIC # Kafka-to-Lakebase Real-Time Mode ingestion
# MAGIC
# MAGIC This standalone Structured Streaming query consumes the pump telemetry topic and writes
# MAGIC validated events directly to Lakebase through the native PostgreSQL sink. It intentionally
# MAGIC owns no alerting or agent logic; those decisions remain in the Databricks App.
# MAGIC
# MAGIC The query runs on dedicated DBR 19 classic compute with Real-Time Mode enabled and stores
# MAGIC its checkpoint in a Unity Catalog volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Runtime dependencies
# MAGIC
# MAGIC The query uses only runtime-provided PySpark APIs. JSON is used to emit structured progress
# MAGIC records, regular expressions validate deploy-time identifiers before Spark starts, and the
# MAGIC monotonic clock bounds diagnostic runs without depending on wall-clock changes.

# COMMAND ----------

"""Standalone Spark Real-Time Mode ingestion from Kafka to Lakebase."""

from __future__ import annotations

import json
import re
import time

from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Declare deployment parameters
# MAGIC
# MAGIC These widgets form the notebook's deployment contract. The Asset Bundle supplies environment-
# MAGIC specific values, while defaults keep an interactive diagnostic run understandable. No secret
# MAGIC credentials are embedded: Kafka is reachable through the workspace network and the Lakebase
# MAGIC sink obtains its credential from the Databricks runtime.

# COMMAND ----------

dbutils.widgets.text("catalog", "classic_stable_realtime_demos_catalog")
dbutils.widgets.text("schema", "asset_reliability_command_center")
dbutils.widgets.text("volume", "runtime")
dbutils.widgets.text("kafka_bootstrap", "63.186.81.193:443")
dbutils.widgets.text("kafka_topic", "asset-reliability-pump-telemetry")
dbutils.widgets.text(
    "lakebase_endpoint",
    "projects/asset-reliability-command-center/branches/production/endpoints/primary",
)
dbutils.widgets.text("lakebase_database", "databricks_postgres")
dbutils.widgets.text("lakebase_table", "asset_reliability.telemetry_events")
dbutils.widgets.text("starting_offsets", "latest")
dbutils.widgets.text("trigger_interval", "5 minutes")
dbutils.widgets.text("diagnostic_seconds", "0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and validate configuration
# MAGIC
# MAGIC The notebook reads widgets exactly once so the active configuration cannot change mid-query.
# MAGIC Unity Catalog and PostgreSQL identifiers are restricted to simple lowercase names before
# MAGIC being interpolated into storage paths or sink options. The checkpoint is stable across
# MAGIC continuous-job replacement and unique to this stream.

# COMMAND ----------

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
KAFKA_BOOTSTRAP = dbutils.widgets.get("kafka_bootstrap")
KAFKA_TOPIC = dbutils.widgets.get("kafka_topic")
LAKEBASE_ENDPOINT = dbutils.widgets.get("lakebase_endpoint")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_TABLE = dbutils.widgets.get("lakebase_table")
DIAGNOSTIC_SECONDS = int(dbutils.widgets.get("diagnostic_seconds"))

for identifier in (CATALOG, SCHEMA, VOLUME):
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", identifier):
        raise ValueError(f"Invalid Unity Catalog identifier: {identifier}")
if not re.fullmatch(r"[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*", LAKEBASE_TABLE):
    raise ValueError("lakebase_table must be schema.table using simple identifiers")

CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/checkpoints/pump_telemetry_rtm"
print("spark.version =", spark.version)
print(
    "spark.databricks.streaming.realTimeMode.enabled =",
    spark.conf.get("spark.databricks.streaming.realTimeMode.enabled", "UNSET"),
)
print("checkpoint =", CHECKPOINT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query observability
# MAGIC
# MAGIC The listener emits source rate, processing rate, latency, and sink-row metrics into the job
# MAGIC logs without altering the streaming data path.

# COMMAND ----------


class ProgressLogger(StreamingQueryListener):
    """Emit compact RTM progress records to the Databricks job logs."""

    def onQueryStarted(self, event):
        print(f"[RTM START] id={event.id}")

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        print(
            "[RTM PROGRESS]",
            json.dumps(
                {
                    "batch_id": progress.get("batchId"),
                    "input_rows": progress.get("numInputRows"),
                    "input_rps": progress.get("inputRowsPerSecond"),
                    "processed_rps": progress.get("processedRowsPerSecond"),
                    "latencies": progress.get("latencies"),
                    "sink_rows": (progress.get("sink") or {}).get("numOutputRows"),
                }
            ),
        )

    def onQueryTerminated(self, event):
        print(f"[RTM TERMINATED] id={event.id} exception={event.exception}")


spark.streams.addListener(ProgressLogger())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decode and validate the telemetry envelope
# MAGIC
# MAGIC An explicit schema prevents runtime inference from a continuously growing source. The schema
# MAGIC is the contract shared with the synthetic producer; changes must remain compatible with the
# MAGIC Lakebase table and the parallel governed Unity Catalog pipeline.

# COMMAND ----------

message_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("scenario_id", StringType(), False),
        StructField("pump_id", StringType(), False),
        StructField("sequence", LongType(), False),
        StructField("event_time", StringType(), False),
        StructField("flow_m3h", DoubleType(), False),
        StructField("discharge_pressure_bar", DoubleType(), False),
        StructField("vibration_mm_s", DoubleType(), False),
        StructField("bearing_temperature_c", DoubleType(), False),
        StructField("motor_current_a", DoubleType(), False),
        StructField("source", StringType(), False),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subscribe to the Kafka telemetry topic
# MAGIC
# MAGIC The source begins at the configured offsets and keeps Kafka partition, offset, and broker
# MAGIC timestamp for traceability. `failOnDataLoss=false` is an intentional synthetic-demo tradeoff:
# MAGIC a broker retention gap must not prevent the live demo from resuming. The governed Lakeflow
# MAGIC pipeline provides separate quality and quarantine visibility for historical analysis.

# COMMAND ----------

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", dbutils.widgets.get("starting_offsets"))
    .option("failOnDataLoss", "false")
    .option("kafka.fetch.max.wait.ms", "50")
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and select operationally valid events
# MAGIC
# MAGIC JSON decoding retains Kafka metadata alongside the business payload. Rows without an event,
# MAGIC scenario, pump, or parseable event time are excluded from the operational store because they
# MAGIC cannot be deduplicated or associated with live state. Measurement-range analysis belongs to
# MAGIC the governed UC pipeline; this path applies only the invariants required by Lakebase.

# COMMAND ----------

telemetry = (
    raw.select(
        F.from_json(F.col("value").cast("string"), message_schema).alias("event"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    .select("event.*", "kafka_timestamp", "kafka_partition", "kafka_offset")
    .withColumn("event_time", F.to_timestamp("event_time"))
    .filter(
        F.col("event_id").isNotNull()
        & F.col("scenario_id").isNotNull()
        & F.col("pump_id").isNotNull()
        & F.col("event_time").isNotNull()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start the native Lakebase sink
# MAGIC
# MAGIC The PostgreSQL sink uses update mode, a persistent checkpoint, small sink batches, and a
# MAGIC Real-Time Mode trigger. `event_id` is the Lakebase primary key, so replayed Kafka records are
# MAGIC idempotent at the destination. The returned query handle is the sole lifecycle authority for
# MAGIC both continuous production and bounded diagnostics.

# COMMAND ----------

query = (
    telemetry.writeStream.format("postgresql")
    .outputMode("update")
    .option("endpoint", LAKEBASE_ENDPOINT)
    .option("database", LAKEBASE_DATABASE)
    .option("dbtable", LAKEBASE_TABLE)
    .option("batchinterval", "100 milliseconds")
    .option("batchsize", "500")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(realTime=dbutils.widgets.get("trigger_interval"))
    .start()
)

print(f"RTM query started: {query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run continuously or return bounded diagnostics
# MAGIC
# MAGIC Production sets `diagnostic_seconds` to zero and waits for termination so the Lakeflow Job can
# MAGIC supervise failures and replacement runs. A positive diagnostic duration stops the query and
# MAGIC returns recent structured progress, including latency and sink-row metrics, as notebook output
# MAGIC suitable for text-based acceptance evidence.

# COMMAND ----------

if DIAGNOSTIC_SECONDS > 0:
    deadline = time.monotonic() + DIAGNOSTIC_SECONDS
    while query.isActive and time.monotonic() < deadline:
        time.sleep(5)
    summary = {
        "query_id": str(query.id),
        "is_active": query.isActive,
        "exception": str(query.exception()) if query.exception() else None,
        "recent_progress": [json.loads(item.json) for item in query.recentProgress[-3:]],
    }
    query.stop()
    dbutils.notebook.exit(json.dumps(summary, default=str))
else:
    query.awaitTermination()
