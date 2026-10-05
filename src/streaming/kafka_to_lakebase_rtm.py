# Databricks notebook source
"""Standalone Spark Real-Time Mode ingestion: Kafka to Lakebase."""

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


class ProgressLogger(StreamingQueryListener):
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

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", dbutils.widgets.get("starting_offsets"))
    .option("failOnDataLoss", "false")
    .option("kafka.fetch.max.wait.ms", "50")
    .load()
)

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
