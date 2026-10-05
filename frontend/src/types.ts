export type PlatformStatus = {
  connection: "connected" | "degraded" | "offline";
  source_label: string;
  latency_ms: number;
  kafka_to_lakebase_latency_ms: number;
  lakebase_to_ui_latency_ms: number;
  end_to_end_latency_ms: number;
  last_event_at: string;
};

export type PumpState = {
  asset_id: string;
  name: string;
  unit: string;
  state: "normal" | "transient" | "warning";
  state_label: string;
  vibration_mm_s: number;
  warning_threshold_mm_s: number;
  flow_m3_h: number;
  current_a: number;
  trend: number[];
  maintenance_label?: string | null;
  active_run_id?: string | null;
  updated_at: string;
};

export type AlertSummary = {
  id: string;
  incident_id: string;
  asset_id: string;
  title: string;
  condition: string;
  status: string;
  status_label: string;
  observed_at: string;
  resolution?: string | null;
};

export type InvocationSummary = {
  id: string;
  incident_id: string;
  asset_id: string;
  title: string;
  status: string;
  status_label: string;
  started_at: string;
  completed_at?: string | null;
  outcome?: string | null;
};

export type ActiveRun = {
  invocation_id: string;
  incident_id: string;
  asset_id: string;
  label: string;
  current_activity: string;
  started_at: string;
};

export type DashboardSnapshot = {
  revision: number;
  scenario: {
    id: string;
    label: string;
    p204_triggered: boolean;
    started_at: string;
  };
  platform: PlatformStatus;
  pumps: PumpState[];
  alerts: AlertSummary[];
  invocations: InvocationSummary[];
  active_runs: ActiveRun[];
  shift_operator: {
    operator?: { email: string; name: string; last_seen_at: string } | null;
    recent_users: { email: string; name: string; last_seen_at: string }[];
    version: number;
    updated_at?: string | null;
    updated_by?: string | null;
  };
  viewer?: { email: string; name: string; is_operator: boolean } | null;
};

export type RuntimeAction = {
  label: string;
  kind: string;
  endpoint?: string | null;
  href?: string | null;
};

export type RuntimeActivity = {
  id: string;
  category: string;
  title: string;
  actor?: string | null;
  status: "running" | "complete" | "waiting" | "failed" | "info";
  status_label: string;
  display_scope?: "all" | "timeline";
  summary?: string | null;
  details: { label: string; value: string }[];
  action?: RuntimeAction | null;
  emitted_at: string;
  started_at?: string | null;
  completed_at?: string | null;
};

export type IncidentDetail = {
  id: string;
  scenario_id: string;
  asset_id: string;
  title: string;
  condition: string;
  trigger_prompt: string;
  status: string;
  status_label: string;
  narrative: string[];
  conversation: {
    id: string;
    role: "agent" | "operator" | "system";
    content: string;
    created_at: string;
  }[];
  activities: RuntimeActivity[];
  totals: {
    input_tokens: number;
    output_tokens: number;
    estimated_cost_usd: number;
    tool_calls: number;
  };
  created_at: string;
  updated_at: string;
};
