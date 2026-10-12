-- Canonical operational schema for the Asset Reliability Command Center.
-- This migration is intentionally idempotent because every App startup applies it.

-- Fleet reference data and scenario lifecycle.
CREATE SCHEMA IF NOT EXISTS asset_reliability;

CREATE TABLE IF NOT EXISTS asset_reliability.pumps (
    pump_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    area_name TEXT NOT NULL,
    asset_class TEXT NOT NULL DEFAULT 'Centrifugal pump',
    criticality TEXT NOT NULL CHECK (criticality IN ('low', 'medium', 'high')),
    next_maintenance_window TIMESTAMPTZ NOT NULL
);

INSERT INTO asset_reliability.pumps
    (pump_id, display_name, area_name, criticality, next_maintenance_window)
VALUES
    ('P-201', 'Cooling Water Pump P-201', 'Cooling loop A', 'medium', now() + interval '12 days'),
    ('P-202', 'Cooling Water Pump P-202', 'Cooling loop A', 'medium', now() + interval '14 days'),
    ('P-203', 'Cooling Water Pump P-203', 'Cooling loop A', 'medium', now() + interval '11 days'),
    ('P-204', 'Cooling Water Pump P-204', 'Cooling loop A', 'high', now() + interval '9 days'),
    ('P-205', 'Cooling Water Pump P-205', 'Cooling loop B', 'medium', now() + interval '13 days'),
    ('P-206', 'Cooling Water Pump P-206', 'Cooling loop B', 'medium', now() + interval '10 days'),
    ('P-207', 'Cooling Water Pump P-207', 'Cooling loop B', 'medium', now() + interval '15 days'),
    ('P-208', 'Cooling Water Pump P-208', 'Cooling loop B', 'medium', now() + interval '12 days')
ON CONFLICT (pump_id) DO UPDATE
SET display_name = EXCLUDED.display_name,
    area_name = EXCLUDED.area_name,
    criticality = EXCLUDED.criticality;

CREATE TABLE IF NOT EXISTS asset_reliability.scenario_runs (
    scenario_id UUID PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('active', 'archived')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ,
    requested_by TEXT,
    p204_excursion_started_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS one_active_asset_reliability_scenario
    ON asset_reliability.scenario_runs ((status))
    WHERE status = 'active';

-- Shared shift-operator identity and immutable handover audit.
CREATE TABLE IF NOT EXISTS asset_reliability.app_users (
    email TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS asset_reliability.shift_operator (
    singleton BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    operator_email TEXT REFERENCES asset_reliability.app_users(email),
    version BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ,
    updated_by TEXT
);

INSERT INTO asset_reliability.shift_operator (singleton)
VALUES (true) ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS asset_reliability.shift_operator_audit (
    audit_id UUID PRIMARY KEY,
    previous_operator_email TEXT,
    new_operator_email TEXT NOT NULL,
    initiated_by TEXT NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    previous_version BIGINT NOT NULL,
    new_version BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS asset_reliability.scenario_commands (
    command_id UUID PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES asset_reliability.scenario_runs(scenario_id),
    command_type TEXT NOT NULL CHECK (
        command_type IN ('restart_scenario', 'start_p204_excursion')
    ),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (
        status IN ('pending', 'claimed', 'completed', 'failed')
    ),
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    requested_by TEXT,
    claimed_at TIMESTAMPTZ,
    claimed_by TEXT,
    lease_expires_at TIMESTAMPTZ,
    claim_version BIGINT NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    completed_at TIMESTAMPTZ,
    error TEXT,
    UNIQUE (scenario_id, command_type)
);

-- Older installations predate command leases. These additive migrations keep an active
-- producer compatible while enabling another producer to reclaim work abandoned by a failed
-- process. Completion is fenced by both owner and claim_version in the producer code.
ALTER TABLE asset_reliability.scenario_commands
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claim_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

-- A command claimed before lease support was deployed has no expiry and would otherwise be
-- stranded forever. Expiring it in place lets the version-fenced producer reclaim it safely.
UPDATE asset_reliability.scenario_commands
SET lease_expires_at = clock_timestamp() - interval '1 second'
WHERE status = 'claimed' AND lease_expires_at IS NULL;

CREATE INDEX IF NOT EXISTS scenario_commands_pending_idx
    ON asset_reliability.scenario_commands (requested_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS scenario_commands_reclaim_idx
    ON asset_reliability.scenario_commands (lease_expires_at)
    WHERE status = 'claimed';

-- Append-oriented telemetry plus the bounded latest-state read model.
CREATE TABLE IF NOT EXISTS asset_reliability.telemetry_events (
    event_id TEXT PRIMARY KEY,
    scenario_id TEXT NOT NULL,
    pump_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    flow_m3h DOUBLE PRECISION NOT NULL,
    discharge_pressure_bar DOUBLE PRECISION NOT NULL,
    vibration_mm_s DOUBLE PRECISION NOT NULL,
    bearing_temperature_c DOUBLE PRECISION NOT NULL,
    motor_current_a DOUBLE PRECISION NOT NULL,
    source TEXT NOT NULL,
    kafka_timestamp TIMESTAMPTZ,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE asset_reliability.telemetry_events
    ALTER COLUMN ingested_at SET DEFAULT clock_timestamp();

CREATE INDEX IF NOT EXISTS telemetry_events_scenario_pump_time_idx
    ON asset_reliability.telemetry_events (scenario_id, pump_id, event_time DESC);

CREATE TABLE IF NOT EXISTS asset_reliability.pump_live_state (
    scenario_id TEXT NOT NULL,
    pump_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    flow_m3h DOUBLE PRECISION NOT NULL,
    discharge_pressure_bar DOUBLE PRECISION NOT NULL,
    vibration_mm_s DOUBLE PRECISION NOT NULL,
    bearing_temperature_c DOUBLE PRECISION NOT NULL,
    motor_current_a DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (scenario_id, pump_id)
);

CREATE TABLE IF NOT EXISTS asset_reliability.alert_rules (
    rule_id TEXT PRIMARY KEY,
    metric TEXT NOT NULL,
    comparison TEXT NOT NULL CHECK (comparison IN ('gt', 'gte', 'lt', 'lte')),
    warning_threshold DOUBLE PRECISION NOT NULL,
    clear_threshold DOUBLE PRECISION NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT true,
    description TEXT NOT NULL
);

INSERT INTO asset_reliability.alert_rules
    (rule_id, metric, comparison, warning_threshold, clear_threshold, description)
VALUES
    (
        'bearing-vibration-warning',
        'vibration_mm_s',
        'gte',
        4.4,
        3.6,
        'Bearing vibration has crossed the fleet warning threshold.'
    )
ON CONFLICT (rule_id) DO UPDATE
SET warning_threshold = EXCLUDED.warning_threshold,
    clear_threshold = EXCLUDED.clear_threshold,
    enabled = true,
    description = EXCLUDED.description;

-- Durable incident, agent execution, conversation, and governed-action journal.
CREATE TABLE IF NOT EXISTS asset_reliability.incidents (
    incident_id UUID PRIMARY KEY,
    scenario_id UUID NOT NULL REFERENCES asset_reliability.scenario_runs(scenario_id),
    pump_id TEXT NOT NULL REFERENCES asset_reliability.pumps(pump_id),
    rule_id TEXT NOT NULL REFERENCES asset_reliability.alert_rules(rule_id),
    status TEXT NOT NULL CHECK (
        status IN ('reviewing', 'waiting_approval', 'resolved_no_action', 'maintenance_scheduled', 'failed')
    ),
    observed_value DOUBLE PRECISION NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    resolution_summary TEXT,
    UNIQUE (scenario_id, pump_id, rule_id)
);

CREATE INDEX IF NOT EXISTS incidents_scenario_time_idx
    ON asset_reliability.incidents (scenario_id, triggered_at DESC);

CREATE TABLE IF NOT EXISTS asset_reliability.agent_runs (
    run_id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES asset_reliability.incidents(incident_id),
    status TEXT NOT NULL CHECK (
        status IN ('queued', 'running', 'waiting_approval', 'completed', 'failed')
    ),
    trigger_prompt TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    pending_approval_url TEXT,
    pending_tool_request JSONB,
    approval_replay_state TEXT CHECK (
        approval_replay_state IN ('pending', 'claimed', 'completed')
    ),
    approval_claimed_at TIMESTAMPTZ,
    approval_claimed_by TEXT,
    approval_claim_id UUID,
    approval_claim_version BIGINT NOT NULL DEFAULT 0,
    total_input_tokens BIGINT NOT NULL DEFAULT 0,
    total_output_tokens BIGINT NOT NULL DEFAULT 0,
    estimated_cost_usd NUMERIC(12, 6) NOT NULL DEFAULT 0,
    tool_call_count INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

ALTER TABLE asset_reliability.agent_runs
    ADD COLUMN IF NOT EXISTS approval_replay_state TEXT,
    ADD COLUMN IF NOT EXISTS approval_claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS approval_claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS approval_claim_id UUID,
    ADD COLUMN IF NOT EXISTS approval_claim_version BIGINT NOT NULL DEFAULT 0;

-- CREATE TABLE constraints are not retroactively applied to an existing live table. Add the
-- replay-state contract explicitly and validate it after the additive column migration.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'agent_runs_approval_replay_state_check'
          AND conrelid = 'asset_reliability.agent_runs'::regclass
    ) THEN
        ALTER TABLE asset_reliability.agent_runs
            ADD CONSTRAINT agent_runs_approval_replay_state_check
            CHECK (approval_replay_state IN ('pending', 'claimed', 'completed')) NOT VALID;
    END IF;
END;
$$;

-- Also finishes validation if an earlier deployment added the constraint as NOT VALID but was
-- interrupted before the scan completed.
ALTER TABLE asset_reliability.agent_runs
    VALIDATE CONSTRAINT agent_runs_approval_replay_state_check;

-- Each successful Gateway replay emits one immutable receipt. The request hash binds the
-- human decision to the exact JSON-RPC bytes that were paused by Unity Gateway without
-- persisting the operator's OBO bearer token.
CREATE TABLE IF NOT EXISTS asset_reliability.governance_receipts (
    receipt_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES asset_reliability.agent_runs(run_id),
    incident_id UUID NOT NULL REFERENCES asset_reliability.incidents(incident_id),
    elicitation_id TEXT NOT NULL UNIQUE,
    request_sha256 TEXT NOT NULL CHECK (length(request_sha256) = 64),
    approving_subject TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('ALLOW', 'DENY')),
    requested_at TIMESTAMPTZ NOT NULL,
    decided_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION asset_reliability.reject_governance_receipt_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'governance receipts are immutable';
END;
$$;

DROP TRIGGER IF EXISTS governance_receipts_are_immutable
    ON asset_reliability.governance_receipts;

CREATE TRIGGER governance_receipts_are_immutable
BEFORE UPDATE OR DELETE ON asset_reliability.governance_receipts
FOR EACH ROW EXECUTE FUNCTION asset_reliability.reject_governance_receipt_mutation();

CREATE TABLE IF NOT EXISTS asset_reliability.activity_events (
    event_id UUID PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES asset_reliability.agent_runs(run_id),
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    title TEXT NOT NULL,
    detail TEXT,
    status TEXT,
    runtime_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, sequence)
);

CREATE INDEX IF NOT EXISTS activity_events_run_sequence_idx
    ON asset_reliability.activity_events (run_id, sequence);

CREATE TABLE IF NOT EXISTS asset_reliability.conversation_messages (
    message_id UUID PRIMARY KEY,
    incident_id UUID NOT NULL REFERENCES asset_reliability.incidents(incident_id),
    sequence INTEGER NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('agent', 'operator', 'system')),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (incident_id, sequence)
);

CREATE INDEX IF NOT EXISTS conversation_messages_incident_sequence_idx
    ON asset_reliability.conversation_messages (incident_id, sequence);

-- Model output is canonical in activity_events. Remove legacy copies that predate the
-- single-source response model while retaining operator and deterministic runtime messages.
DELETE FROM asset_reliability.conversation_messages AS message
USING asset_reliability.agent_runs AS run, asset_reliability.activity_events AS activity
WHERE message.incident_id = run.incident_id
  AND activity.run_id = run.run_id
  AND message.role = 'agent'
  AND activity.event_type = 'model_call'
  AND activity.runtime_metadata ->> 'summary' = message.content;

CREATE TABLE IF NOT EXISTS asset_reliability.work_orders (
    work_order_id TEXT PRIMARY KEY,
    incident_id UUID NOT NULL UNIQUE REFERENCES asset_reliability.incidents(incident_id),
    pump_id TEXT NOT NULL REFERENCES asset_reliability.pumps(pump_id),
    action TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('draft', 'submitted', 'scheduled')),
    scheduled_for TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at TIMESTAMPTZ,
    submitted_by TEXT,
    governance_receipt JSONB
);

CREATE TABLE IF NOT EXISTS asset_reliability.component_heartbeats (
    component_name TEXT PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Maintain latest pump state and notify the App after every accepted telemetry write.
-- The sequence guard prevents a delayed Kafka record from replacing newer live state.
CREATE OR REPLACE FUNCTION asset_reliability.apply_telemetry_event()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO asset_reliability.pump_live_state (
        scenario_id, pump_id, event_id, sequence, event_time, ingested_at,
        flow_m3h, discharge_pressure_bar, vibration_mm_s,
        bearing_temperature_c, motor_current_a
    ) VALUES (
        NEW.scenario_id, NEW.pump_id, NEW.event_id, NEW.sequence,
        NEW.event_time, NEW.ingested_at, NEW.flow_m3h,
        NEW.discharge_pressure_bar, NEW.vibration_mm_s,
        NEW.bearing_temperature_c, NEW.motor_current_a
    )
    ON CONFLICT (scenario_id, pump_id) DO UPDATE
    SET event_id = EXCLUDED.event_id,
        sequence = EXCLUDED.sequence,
        event_time = EXCLUDED.event_time,
        ingested_at = EXCLUDED.ingested_at,
        flow_m3h = EXCLUDED.flow_m3h,
        discharge_pressure_bar = EXCLUDED.discharge_pressure_bar,
        vibration_mm_s = EXCLUDED.vibration_mm_s,
        bearing_temperature_c = EXCLUDED.bearing_temperature_c,
        motor_current_a = EXCLUDED.motor_current_a
    WHERE EXCLUDED.sequence > asset_reliability.pump_live_state.sequence;

    PERFORM pg_notify(
        'asset_reliability_telemetry',
        json_build_object(
            'scenario_id', NEW.scenario_id,
            'pump_id', NEW.pump_id,
            'sequence', NEW.sequence,
            'ingested_at', NEW.ingested_at
        )::text
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS telemetry_event_applied
    ON asset_reliability.telemetry_events;

CREATE TRIGGER telemetry_event_applied
AFTER INSERT OR UPDATE ON asset_reliability.telemetry_events
FOR EACH ROW EXECUTE FUNCTION asset_reliability.apply_telemetry_event();

-- The streaming table is write-heavy and retention deletes continuously create dead tuples.
ALTER TABLE asset_reliability.telemetry_events SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 500,
    autovacuum_vacuum_cost_delay = 0
);
