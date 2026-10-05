import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  AlertSummary,
  DashboardSnapshot,
  IncidentDetail,
  InvocationSummary,
  PumpState,
  RuntimeAction,
  RuntimeActivity
} from "./types";

async function jsonRequest<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  if (!response.ok) {
    const body = await response.json().catch(() => ({ detail: "Request failed" }));
    throw new Error(body.detail ?? "Request failed");
  }
  return response.json() as Promise<T>;
}

function timeLabel(value: string) {
  return new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(value));
}

function Sparkline({ pump }: { pump: PumpState }) {
  const values = pump.trend.length > 1 ? pump.trend : [pump.vibration_mm_s, pump.vibration_mm_s];
  const min = Math.min(...values, 2.4);
  const max = Math.max(...values, pump.warning_threshold_mm_s + 0.5);
  const range = Math.max(max - min, 0.1);
  const points = values
    .map((value, index) => {
      const x = (index / Math.max(values.length - 1, 1)) * 180;
      const y = 48 - ((value - min) / range) * 42;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    })
    .join(" ");
  const thresholdY = 48 - ((pump.warning_threshold_mm_s - min) / range) * 42;
  return (
    <svg className="sparkline" viewBox="0 0 180 52" preserveAspectRatio="none" aria-hidden="true">
      <line className="threshold-line" x1="0" x2="180" y1={thresholdY} y2={thresholdY} />
      <polyline className={`signal signal-${pump.state}`} points={points} />
    </svg>
  );
}

function StatusPill({ status, label }: { status: string; label: string }) {
  return <span className={`status-pill status-${status}`}>{label}</span>;
}

const EVENT_ICONS: Record<string, ReactNode> = {
  model_call: "✦",
  skill: "◇",
  mcp: "⇄",
  tool_call: "⌁",
  tool_result: "✓",
  approval: "⚿",
  agent: "AI",
  operator: "●",
  runtime: "!"
};

function EventIcon({ category }: { category: string }) {
  return <span className={`event-icon icon-${category}`}>{EVENT_ICONS[category] ?? "·"}</span>;
}

function AgentMessage({ content }: { content: string }) {
  return (
    <div className="structured-message">
      {content.split("\n").map((line, index) => {
        const match = line.match(/^\*\*(Finding|Evidence|Risk|Action):\*\*\s*(.*)$/);
        return match ? <p key={index}><strong>{match[1]}</strong><span>{match[2]}</span></p> : <p key={index}>{line}</p>;
      })}
    </div>
  );
}

type HistoryRecord = {
  alert: AlertSummary;
  invocation?: InvocationSummary;
};

function App() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [incident, setIncident] = useState<IncidentDetail | null>(null);
  const [selectedIncidentId, setSelectedIncidentId] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<"fleet" | "history">("fleet");
  const [incidentView, setIncidentView] = useState<"conversation" | "timeline">("conversation");
  const [menuOpen, setMenuOpen] = useState(false);
  const [followUp, setFollowUp] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [backendMode, setBackendMode] = useState<"local" | "lakebase">("local");
  const [fontScale, setFontScale] = useState(() => {
    const saved = Number(localStorage.getItem("command-center-font-scale"));
    return [100, 110, 120, 130].includes(saved) ? saved : 100;
  });
  const conversationRef = useRef<HTMLDivElement>(null);

  const refreshIncident = useCallback(async (incidentId: string) => {
    const detail = await jsonRequest<IncidentDetail>(`/api/incidents/${incidentId}`);
    setIncident(detail);
  }, []);

  useEffect(() => {
    document.documentElement.style.setProperty("--font-scale", String(fontScale / 100));
    localStorage.setItem("command-center-font-scale", String(fontScale));
  }, [fontScale]);

  useEffect(() => {
    let source: EventSource | null = null;
    let disposed = false;
    void (async () => {
      try {
        const [initial, health] = await Promise.all([
          jsonRequest<DashboardSnapshot>("/api/dashboard"),
          jsonRequest<{ adapter: "local" | "lakebase" }>("/api/health")
        ]);
        if (disposed) return;
        setSnapshot(initial);
        setBackendMode(health.adapter);
        source = new EventSource(`/api/events?after=${initial.revision}`);
        source.onmessage = (event) => {
          setSnapshot(JSON.parse(event.data) as DashboardSnapshot);
          setError(null);
        };
        source.onerror = () => setError("Live updates are reconnecting");
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : "Unable to load command center");
      }
    })();
    return () => {
      disposed = true;
      source?.close();
    };
  }, []);

  useEffect(() => {
    if (!selectedIncidentId || !snapshot) return;
    void refreshIncident(selectedIncidentId).catch((caught) => {
      setError(caught instanceof Error ? caught.message : "Unable to refresh incident");
    });
  }, [refreshIncident, selectedIncidentId, snapshot?.revision]);

  useEffect(() => {
    const conversation = conversationRef.current;
    if (conversation && incidentView === "conversation") conversation.scrollTop = conversation.scrollHeight;
  }, [incident?.activities.length, incident?.conversation.length, incidentView]);

  const activeByAsset = useMemo(
    () => new Map(snapshot?.active_runs.map((run) => [run.asset_id, run]) ?? []),
    [snapshot?.active_runs]
  );

  const historyRecords = useMemo<HistoryRecord[]>(() => {
    if (!snapshot) return [];
    const invocationByIncident = new Map(snapshot.invocations.map((run) => [run.incident_id, run]));
    return snapshot.alerts.map((alert) => ({ alert, invocation: invocationByIncident.get(alert.incident_id) }));
  }, [snapshot]);

  const openIncident = async (incidentId: string) => {
    setSelectedIncidentId(incidentId);
    setIncidentView("conversation");
    setIncident(null);
    try {
      await refreshIncident(incidentId);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Unable to open incident");
    }
  };

  const restart = async () => {
    setBusy(true);
    setMenuOpen(false);
    setError(null);
    setIncident(null);
    setSelectedIncidentId(null);
    try {
      await jsonRequest("/api/scenario/restart", { method: "POST" });
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Unable to restart scenario");
    } finally {
      setBusy(false);
    }
  };

  const trigger = async () => {
    setBusy(true);
    setMenuOpen(false);
    setError(null);
    try {
      const response = await jsonRequest<{ incident_id?: string }>("/api/scenario/trigger-p204", { method: "POST" });
      if (response.incident_id) await openIncident(response.incident_id);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Unable to trigger excursion");
    } finally {
      setBusy(false);
    }
  };

  const runAction = async (action: RuntimeAction) => {
    if (action.href) window.open(action.href, "_blank", "noopener,noreferrer");
    if (!action.endpoint) return;
    setBusy(true);
    try {
      await jsonRequest(action.endpoint, { method: "POST" });
      if (selectedIncidentId) await refreshIncident(selectedIncidentId);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Action failed");
    } finally {
      setBusy(false);
    }
  };

  const submitFollowUp = async (event: FormEvent) => {
    event.preventDefault();
    if (!selectedIncidentId || !followUp.trim()) return;
    setBusy(true);
    try {
      await jsonRequest(`/api/incidents/${selectedIncidentId}/follow-up`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: followUp.trim() })
      });
      setFollowUp("");
      await refreshIncident(selectedIncidentId);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Follow-up failed");
    } finally {
      setBusy(false);
    }
  };

  if (!snapshot) {
    return <div className="loading-screen"><div className="loading-mark" />Connecting to live operations…</div>;
  }

  const conversationItems = incident ? [
    ...incident.activities.map((activity) => ({ kind: "activity" as const, at: activity.emitted_at, activity })),
    ...incident.conversation.map((turn) => ({ kind: "message" as const, at: turn.created_at, turn }))
  ].sort((a, b) => new Date(a.at).getTime() - new Date(b.at).getTime()) : [];

  return (
    <main className="command-center">
      {backendMode === "lakebase" && <div className="live-backend-banner" role="status"><strong>LIVE BACKEND</strong><span>Controls and approvals modify the shared Databricks demo environment.</span></div>}
      <header className="command-header">
        <div className="brand-lockup"><div className="brand-glyph"><span /></div><div><p>Databricks Intelligence Platform</p><h1>Asset Reliability Command Center</h1></div></div>
        <div className="latency-panel">
          <div className={`connection-dot connection-${snapshot.platform.connection}`} aria-label={snapshot.platform.connection} />
          <div><span>Kafka → Lakebase</span><strong>{snapshot.platform.kafka_to_lakebase_latency_ms} ms</strong></div>
          <div><span>Lakebase → UI</span><strong>{snapshot.platform.lakebase_to_ui_latency_ms} ms</strong></div>
          <div className="latency-total"><span>End to end</span><strong>{snapshot.platform.end_to_end_latency_ms} ms</strong></div>
        </div>
        <div className="demo-menu-wrap">
          <button className="menu-button" aria-label="Open demo controls" aria-expanded={menuOpen} onClick={() => setMenuOpen(!menuOpen)}><i /><i /><i /></button>
          {menuOpen && <div className="demo-menu"><p>Demo controls</p><div className="text-size-control"><span>Text size</span><button aria-label="Decrease text size" onClick={() => setFontScale((value) => Math.max(100, value - 10))} disabled={fontScale === 100}>−</button><strong>{fontScale}%</strong><button aria-label="Increase text size" onClick={() => setFontScale((value) => Math.min(130, value + 10))} disabled={fontScale === 130}>+</button></div><button onClick={() => void trigger()} disabled={busy || snapshot.scenario.p204_triggered}>Trigger P-204 excursion<span>Start the governed failure scenario</span></button><button className="menu-danger" onClick={() => void restart()} disabled={busy}>Clear & restart<span>Reset all scenario activity</span></button></div>}
        </div>
      </header>

      {error && <div className="global-error">{error}</div>}

      <nav className="primary-tabs" aria-label="Command center views">
        <button className={activeView === "fleet" ? "active" : ""} onClick={() => setActiveView("fleet")}>Live fleet</button>
        <button className={activeView === "history" ? "active" : ""} onClick={() => setActiveView("history")}>History <span>{snapshot.alerts.length}</span></button>
      </nav>

      {activeView === "fleet" ? (
        <section className="fleet-section">
          <div className="section-heading"><div><p>Live fleet</p><h2>Comparable pump assets</h2></div><span className="streaming-label"><i /> Streaming now</span></div>
          <div className="pump-grid">
            {snapshot.pumps.map((pump) => {
              const active = activeByAsset.get(pump.asset_id);
              const relatedAlert = snapshot.alerts.find((alert) => alert.asset_id === pump.asset_id);
              return (
                <article key={pump.asset_id} className={`pump-card pump-${pump.state} ${relatedAlert ? "pump-clickable" : ""}`} onClick={() => relatedAlert && void openIncident(relatedAlert.incident_id)}>
                  <div className="pump-card-head"><div><strong>{pump.asset_id}</strong><span>{pump.name}</span></div><StatusPill status={pump.state} label={pump.state_label} /></div>
                  <div className="pump-signal"><div><span>Vibration</span><strong>{pump.vibration_mm_s.toFixed(2)} <small>mm/s</small></strong></div><Sparkline pump={pump} /></div>
                  <div className="pump-metrics"><span><small>Flow</small>{pump.flow_m3_h.toFixed(1)} m³/h</span><span><small>Current</small>{pump.current_a.toFixed(1)} A</span><span><small>Threshold</small>{pump.warning_threshold_mm_s.toFixed(1)} mm/s</span></div>
                  {active && <div className="agent-active"><div className="agent-orbit"><i /></div><div><strong>{active.label}</strong><span>{active.current_activity}</span></div></div>}
                  {pump.maintenance_label && <div className="maintenance-tag">✓ {pump.maintenance_label}</div>}
                </article>
              );
            })}
          </div>
        </section>
      ) : (
        <section className="history-page">
          <div className="history-page-header"><div><p>Operational record</p><h2>Alert & agent history</h2><span>One record for every detected condition and autonomous review.</span></div><strong>{historyRecords.length} incidents</strong></div>
          <div className="history-table-head"><span>Asset & condition</span><span>Alert</span><span>Agent execution</span><span>Outcome</span><span>Observed</span><i /></div>
          <div className="history-records">
            {!historyRecords.length && <div className="empty-row">Incidents will appear here as conditions are detected.</div>}
            {historyRecords.map(({ alert, invocation }) => (
              <button className="history-record" key={alert.id} onClick={() => void openIncident(alert.incident_id)}>
                <div className="history-asset"><span className={`row-marker marker-${alert.status}`} /><div><strong>{alert.asset_id}</strong><span>{alert.title}</span></div></div>
                <StatusPill status={alert.status} label={alert.status_label} />
                {invocation ? <StatusPill status={invocation.status} label={invocation.status_label} /> : <span>—</span>}
                <span className="history-outcome">{invocation?.outcome ?? alert.resolution ?? "Review in progress"}</span>
                <time>{timeLabel(alert.observed_at)}</time><span className="row-chevron">›</span>
              </button>
            ))}
          </div>
        </section>
      )}

      {selectedIncidentId && <div className="overlay" role="dialog" aria-modal="true" aria-label="Incident review">
        <div className="overlay-backdrop" onClick={() => setSelectedIncidentId(null)} />
        <section className="incident-review">
          {!incident ? <div className="incident-loading">Loading incident review…</div> : <>
            <header className="incident-header"><div><p>Incident review · {incident.asset_id}</p><h2>{incident.title}</h2></div><div className="view-toggle"><button className={incidentView === "conversation" ? "active" : ""} onClick={() => setIncidentView("conversation")}>Conversation</button><button className={incidentView === "timeline" ? "active" : ""} onClick={() => setIncidentView("timeline")}>Execution timeline</button></div><StatusPill status={incident.status} label={incident.status_label} /><button className="close-button" onClick={() => setSelectedIncidentId(null)} aria-label="Close incident review">×</button></header>
            <div className="incident-workspace">
              <section className="initiating-prompt"><div className="prompt-icon">↗</div><div><strong>Initiating prompt</strong><p>{incident.trigger_prompt}</p></div></section>
              {incidentView === "conversation" ? <div className="unified-conversation" ref={conversationRef}>
                {!conversationItems.length && <div className="timeline-empty"><i />Waiting for the first runtime event…</div>}
                {conversationItems.map((item) => item.kind === "activity" ? <ActivityCard key={item.activity.id} activity={item.activity} busy={busy} runAction={runAction} /> : <article className={`chat-event chat-${item.turn.role}`} key={item.turn.id}><EventIcon category={item.turn.role} /><div><div className="chat-meta"><strong>{item.turn.role === "operator" ? "You" : "Reliability agent"}</strong><time>{timeLabel(item.turn.created_at)}</time></div>{item.turn.role === "agent" ? <AgentMessage content={item.turn.content} /> : <p>{item.turn.content}</p>}</div></article>)}
              </div> : <ExecutionTimeline activities={incident.activities} />}
              <form className="follow-up" onSubmit={submitFollowUp}><label htmlFor="follow-up">Ask a follow-up</label><div><input id="follow-up" value={followUp} onChange={(event) => setFollowUp(event.target.value)} placeholder="Ask about evidence, risk, or the response…" disabled={busy} /><button disabled={busy || !followUp.trim()}>Send</button></div></form>
            </div>
            <footer className="incident-footer"><div><span>Total tokens</span><strong>{(incident.totals.input_tokens + incident.totals.output_tokens).toLocaleString()}</strong></div><div><span>Estimated cost</span><strong>${incident.totals.estimated_cost_usd.toFixed(4)}</strong></div><div><span>Tool calls</span><strong>{incident.totals.tool_calls}</strong></div><button onClick={() => setSelectedIncidentId(null)}>Close review</button></footer>
          </>}
        </section>
      </div>}
    </main>
  );
}

function gatewayDecision(activity: RuntimeActivity) {
  const decision = activity.details.find((detail) => detail.label.toLowerCase() === "decision")?.value;
  return decision && ["ALLOW", "ASK", "DENY"].includes(decision) ? decision : null;
}

function ActivityCard({ activity, busy, runAction, compact = false }: { activity: RuntimeActivity; busy: boolean; runAction: (action: RuntimeAction) => Promise<void>; compact?: boolean }) {
  const category = activity.status === "waiting" ? "approval" : activity.category;
  const policy = gatewayDecision(activity);
  const runtimeLabel = { running: "Running", complete: "Completed", waiting: "Waiting", failed: "Failed", info: "Information" }[activity.status];
  const details = activity.details.filter((detail) => !["decision", "execution", "input tokens", "output tokens"].includes(detail.label.toLowerCase()));
  const tokenTotal = activity.category === "model_call" ? activity.details.filter((detail) => /tokens/i.test(detail.label)).reduce((sum, detail) => sum + Number(detail.value.replaceAll(",", "")), 0) : 0;
  return <article className={`chat-event runtime-event event-${activity.status} ${compact ? "compact-card" : ""}`}><EventIcon category={category} /><div className="event-content"><div className="event-kicker"><span>{activity.category.replace("_", " ")}</span><time>{timeLabel(activity.emitted_at)}</time></div><div className="event-title"><div><strong>{activity.title}</strong>{activity.actor && <span>{activity.actor}</span>}</div><StatusPill status={activity.status} label={runtimeLabel} /></div>{activity.summary && <p>{activity.summary}</p>}{details.length > 0 && <dl>{details.map((detail) => <div key={`${activity.id}-${detail.label}`}><dt>{detail.label}</dt><dd>{detail.value}</dd></div>)}</dl>}{policy && <section className={`gateway-policy policy-${policy.toLowerCase()}`}><strong>Unity Gateway policy applied</strong><span>{policy}</span><p>{activity.details.find((detail) => detail.label.toLowerCase() === "execution")?.value ?? (policy === "ALLOW" ? "Execution occurred" : "Execution did not occur")}</p></section>}{activity.action && <button className="runtime-action" disabled={busy} onClick={() => void runAction(activity.action!)}>{activity.action.label}</button>}<footer className="event-footer">{tokenTotal > 0 && <span>Total tokens <strong>{tokenTotal.toLocaleString()}</strong></span>}</footer></div></article>;
}

function ExecutionTimeline({ activities }: { activities: RuntimeActivity[] }) {
  const shown = activities.filter((activity) => ["model_call", "skill", "tool_call", "tool_result", "mcp"].includes(activity.category));
  const start = shown.length ? Math.min(...shown.map((activity) => new Date(activity.started_at ?? activity.emitted_at).getTime())) : 0;
  const end = shown.length ? Math.max(...shown.map((activity) => new Date(activity.completed_at ?? activity.emitted_at).getTime()), start + 1000) : start + 1000;
  const range = Math.max(end - start, 1000);
  const lanes = [{ label: "LLM calls", categories: ["model_call"] }, { label: "Skill reads", categories: ["skill"] }, { label: "Tool calls", categories: ["tool_call", "tool_result", "mcp"] }];
  return <div className="execution-view"><div className="execution-scale"><span>Start</span><span>Agent execution</span><span>Latest event</span></div>{shown.length === 0 ? <div className="timeline-empty">No runtime activity yet.</div> : lanes.map((lane) => <section className="gantt-lane" key={lane.label}><h3>{lane.label}</h3><div className="execution-track">{shown.filter((activity) => lane.categories.includes(activity.category)).map((activity) => {
    const activityStart = new Date(activity.started_at ?? activity.emitted_at).getTime();
    const activityEnd = new Date(activity.completed_at ?? activity.emitted_at).getTime();
    const left = Math.min(88, ((activityStart - start) / range) * 88);
    const width = activity.status === "waiting" ? 3 : Math.max(2, Math.min(25, ((Math.max(activityEnd, activityStart + 250) - activityStart) / range) * 88));
    return <span className={`execution-bar category-${activity.category} bar-${activity.status}`} key={activity.id} style={{ left: `${left}%`, width: `${width}%` }} tabIndex={0}><i>{activity.title}</i><span className="gantt-popover"><ActivityCard activity={activity} busy={false} runAction={async () => undefined} compact /></span></span>;
  })}</div></section>)}</div>;
}

export default App;
