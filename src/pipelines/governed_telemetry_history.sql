-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Governed pump telemetry history
-- MAGIC
-- MAGIC This Lakeflow Spark Declarative Pipeline is the governed analytical branch of the
-- MAGIC Asset Reliability Command Center. It consumes the same synthetic Kafka stream as the
-- MAGIC operational RTM job, but it writes durable Unity Catalog datasets for quality
-- MAGIC monitoring and natural-language analysis. The pipeline does not write to Lakebase and
-- MAGIC does not participate in alerting, agent execution, or App control flows.
-- MAGIC
-- MAGIC The bundle supplies `kafka_bootstrap`, `kafka_topic`, and
-- MAGIC `kafka_starting_offsets`. No broker address or topic is embedded in this source.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze: lossless Kafka envelope
-- MAGIC
-- MAGIC Bronze preserves every broker record as text plus Kafka provenance. Parsing and
-- MAGIC business validation intentionally happen downstream so malformed messages remain
-- MAGIC inspectable. `(kafka_topic, kafka_partition, kafka_offset)` identifies the source
-- MAGIC record; `ingested_at` records when Lakeflow first observed it.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE telemetry_bronze (
  message_key STRING COMMENT 'Kafka message key; the producer uses the pump identifier.',
  raw_payload STRING COMMENT 'Unmodified UTF-8 JSON payload received from Kafka.',
  kafka_topic STRING COMMENT 'Kafka topic from which the message was consumed.',
  kafka_partition INT COMMENT 'Kafka partition containing the source message.',
  kafka_offset BIGINT COMMENT 'Monotonic Kafka offset within the source partition.',
  kafka_timestamp TIMESTAMP COMMENT 'Timestamp assigned to the message by Kafka.',
  ingested_at TIMESTAMP COMMENT 'Timestamp at which the Lakeflow pipeline observed the record.'
)
COMMENT 'Raw synthetic pump telemetry and Kafka provenance; restricted to pipeline operators.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'pipelines.autoOptimize.managed' = 'true'
)
AS
SELECT
  CAST(key AS STRING) AS message_key,
  CAST(value AS STRING) AS raw_payload,
  topic AS kafka_topic,
  partition AS kafka_partition,
  offset AS kafka_offset,
  timestamp AS kafka_timestamp,
  current_timestamp() AS ingested_at
FROM STREAM read_kafka(
  bootstrapServers => '${kafka_bootstrap}',
  subscribe => '${kafka_topic}',
  startingOffsets => '${kafka_starting_offsets}',
  failOnDataLoss => 'true'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Shared parsing contract
-- MAGIC
-- MAGIC The temporary view parses the version-controlled producer envelope once for both
-- MAGIC valid and quarantined outputs. `from_json` returns `NULL` for malformed JSON. Numeric
-- MAGIC values use `DOUBLE` here because device readings are continuous measurements; the
-- MAGIC original sequence is retained only for engineering traceability and is not exposed to
-- MAGIC Genie.

-- COMMAND ----------

CREATE TEMPORARY VIEW telemetry_parsed
AS
SELECT
  parsed.event_id,
  parsed.scenario_id,
  parsed.pump_id,
  parsed.sequence,
  parsed.event_time,
  parsed.flow_m3h,
  parsed.discharge_pressure_bar,
  parsed.vibration_mm_s,
  parsed.bearing_temperature_c,
  parsed.motor_current_a,
  parsed.source,
  message_key,
  raw_payload,
  kafka_topic,
  kafka_partition,
  kafka_offset,
  kafka_timestamp,
  ingested_at
FROM (
  SELECT
    from_json(
      raw_payload,
      'event_id STRING, scenario_id STRING, pump_id STRING, sequence BIGINT, event_time TIMESTAMP, flow_m3h DOUBLE, discharge_pressure_bar DOUBLE, vibration_mm_s DOUBLE, bearing_temperature_c DOUBLE, motor_current_a DOUBLE, source STRING'
    ) AS parsed,
    *
  FROM STREAM(telemetry_bronze)
) decoded;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver: typed, validated telemetry
-- MAGIC
-- MAGIC Silver is the durable analytical record. Expectations drop records that cannot be
-- MAGIC interpreted safely while recording violation counts in the Lakeflow event log. The
-- MAGIC ranges are deliberately broad physical-sanity limits, not alert thresholds: business
-- MAGIC anomalies such as high vibration remain valid telemetry and must not be filtered out.
-- MAGIC
-- MAGIC `scenario_id` and Kafka provenance stay in Silver for engineering operations, but the
-- MAGIC curated Genie view below omits them so business users see one continuous history.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE telemetry_silver (
  event_id STRING COMMENT 'Deterministic unique identifier assigned by the synthetic producer.',
  scenario_id STRING COMMENT 'Internal producer-run identifier retained for engineering support.',
  pump_id STRING COMMENT 'Fleet asset identifier, for example P-204.',
  sequence BIGINT COMMENT 'Producer sequence number within its internal run.',
  event_time TIMESTAMP COMMENT 'UTC timestamp at which the measurement was generated.',
  flow_m3h DOUBLE COMMENT 'Volumetric flow in cubic metres per hour.',
  discharge_pressure_bar DOUBLE COMMENT 'Pump discharge pressure in bar.',
  vibration_mm_s DOUBLE COMMENT 'Bearing vibration velocity in millimetres per second.',
  bearing_temperature_c DOUBLE COMMENT 'Bearing temperature in degrees Celsius.',
  motor_current_a DOUBLE COMMENT 'Motor current in amperes.',
  source STRING COMMENT 'Telemetry producer identifier.',
  kafka_topic STRING COMMENT 'Kafka topic carrying the event.',
  kafka_partition INT COMMENT 'Kafka partition containing the event.',
  kafka_offset BIGINT COMMENT 'Kafka offset of the event within its partition.',
  kafka_timestamp TIMESTAMP COMMENT 'Timestamp assigned by Kafka.',
  ingested_at TIMESTAMP COMMENT 'Timestamp at which Lakeflow ingested the event.',
  CONSTRAINT valid_event_id EXPECT (event_id RLIKE '^[0-9a-fA-F-]{36}$') ON VIOLATION DROP ROW,
  CONSTRAINT valid_pump_id EXPECT (pump_id RLIKE '^P-[0-9]{3}$') ON VIOLATION DROP ROW,
  CONSTRAINT required_event_time EXPECT (event_time IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT required_sequence EXPECT (sequence IS NOT NULL AND sequence >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT plausible_flow EXPECT (flow_m3h BETWEEN 0.0 AND 500.0) ON VIOLATION DROP ROW,
  CONSTRAINT plausible_pressure EXPECT (discharge_pressure_bar BETWEEN 0.0 AND 50.0) ON VIOLATION DROP ROW,
  CONSTRAINT plausible_vibration EXPECT (vibration_mm_s BETWEEN 0.0 AND 100.0) ON VIOLATION DROP ROW,
  CONSTRAINT plausible_temperature EXPECT (bearing_temperature_c BETWEEN -50.0 AND 250.0) ON VIOLATION DROP ROW,
  CONSTRAINT plausible_current EXPECT (motor_current_a BETWEEN 0.0 AND 500.0) ON VIOLATION DROP ROW
)
CLUSTER BY (pump_id, event_time)
COMMENT 'Typed and validated synthetic pump telemetry with internal ingestion provenance.'
TBLPROPERTIES (
  'quality' = 'silver',
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  event_id,
  scenario_id,
  pump_id,
  sequence,
  event_time,
  flow_m3h,
  discharge_pressure_bar,
  vibration_mm_s,
  bearing_temperature_c,
  motor_current_a,
  source,
  kafka_topic,
  kafka_partition,
  kafka_offset,
  kafka_timestamp,
  ingested_at
FROM STREAM(telemetry_parsed);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Quarantine: rejected records with reasons
-- MAGIC
-- MAGIC Quarantine applies the same predicates as Silver and retains only failures. The
-- MAGIC comma-separated reason makes malformed or physically impossible messages readable as
-- MAGIC text evidence. This table is intentionally not granted to the App access group.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE telemetry_quarantine
COMMENT 'Rejected telemetry payloads and explicit validation reasons; restricted to operators.'
TBLPROPERTIES ('quality' = 'quarantine')
AS
SELECT
  raw_payload,
  message_key,
  kafka_topic,
  kafka_partition,
  kafka_offset,
  kafka_timestamp,
  ingested_at,
  concat_ws(', ',
    CASE WHEN event_id IS NULL OR NOT event_id RLIKE '^[0-9a-fA-F-]{36}$' THEN 'invalid event_id' END,
    CASE WHEN pump_id IS NULL OR NOT pump_id RLIKE '^P-[0-9]{3}$' THEN 'invalid pump_id' END,
    CASE WHEN event_time IS NULL THEN 'missing event_time' END,
    CASE WHEN sequence IS NULL OR sequence < 0 THEN 'invalid sequence' END,
    CASE WHEN flow_m3h IS NULL OR NOT flow_m3h BETWEEN 0.0 AND 500.0 THEN 'invalid flow_m3h' END,
    CASE WHEN discharge_pressure_bar IS NULL OR NOT discharge_pressure_bar BETWEEN 0.0 AND 50.0 THEN 'invalid discharge_pressure_bar' END,
    CASE WHEN vibration_mm_s IS NULL OR NOT vibration_mm_s BETWEEN 0.0 AND 100.0 THEN 'invalid vibration_mm_s' END,
    CASE WHEN bearing_temperature_c IS NULL OR NOT bearing_temperature_c BETWEEN -50.0 AND 250.0 THEN 'invalid bearing_temperature_c' END,
    CASE WHEN motor_current_a IS NULL OR NOT motor_current_a BETWEEN 0.0 AND 500.0 THEN 'invalid motor_current_a' END
  ) AS rejection_reasons
FROM STREAM(telemetry_parsed)
WHERE
  event_id IS NULL OR NOT event_id RLIKE '^[0-9a-fA-F-]{36}$'
  OR pump_id IS NULL OR NOT pump_id RLIKE '^P-[0-9]{3}$'
  OR event_time IS NULL
  OR sequence IS NULL OR sequence < 0
  OR flow_m3h IS NULL OR NOT flow_m3h BETWEEN 0.0 AND 500.0
  OR discharge_pressure_bar IS NULL OR NOT discharge_pressure_bar BETWEEN 0.0 AND 50.0
  OR vibration_mm_s IS NULL OR NOT vibration_mm_s BETWEEN 0.0 AND 100.0
  OR bearing_temperature_c IS NULL OR NOT bearing_temperature_c BETWEEN -50.0 AND 250.0
  OR motor_current_a IS NULL OR NOT motor_current_a BETWEEN 0.0 AND 500.0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Curated Genie surface
-- MAGIC
-- MAGIC This persistent Unity Catalog view is the only data source configured for the Fleet
-- MAGIC Telemetry Analyst. It presents validated measurements as one chronological history and
-- MAGIC deliberately excludes producer-control identifiers, broker coordinates, and command or
-- MAGIC agent state. Restarting the command-center demonstration therefore has no effect on the
-- MAGIC history visible to Genie.

-- COMMAND ----------

CREATE VIEW pump_telemetry_history
COMMENT 'Continuous validated pump telemetry history curated for natural-language fleet analysis.'
TBLPROPERTIES (
  'quality' = 'gold',
  'purpose' = 'genie-telemetry-analysis'
)
AS
SELECT
  event_time,
  pump_id,
  flow_m3h,
  discharge_pressure_bar,
  vibration_mm_s,
  bearing_temperature_c,
  motor_current_a,
  source,
  ingested_at
FROM telemetry_silver;
