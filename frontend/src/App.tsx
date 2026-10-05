import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  DashboardSnapshot,
  IncidentDetail,
  PumpState,
  RuntimeAction
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

function App() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [incident, setIncident] = useState<IncidentDetail | null>(null);
  const [selectedIncidentId, setSelectedIncidentId] = useState<string | null>(null);
  const [followUp, setFollowUp] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const timelineRef = useRef<HTMLDivElement>(null);

  const refreshIncident = useCallback(async (incidentId: string) => {
    const detail = await jsonRequest<IncidentDetail>(`/api/incidents/${incidentId}`);
    setIncident(detail);
  }, []);

  useEffect(() => {
    let source: EventSource | null = null;
    let disposed = false;
    void (async () => {
      try {
        const initial = await jsonRequest<DashboardSnapshot>("/api/dashboard");
        if (disposed) return;
        setSnapshot(initial);
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
    const timeline = timelineRef.current;
    if (timeline) timeline.scrollTop = timeline.scrollHeight;
  }, [incident?.activities.length]);

  const activeByAsset = useMemo(
    () => new Map(snapshot?.active_runs.map((run) => [run.asset_id, run]) ?? []),
    [snapshot?.active_runs]
  );

  const openIncident = async (incidentId: string) => {
    setSelectedIncidentId(incidentId);
    setIncident(null);
    try {
      await refreshIncident(incidentId);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Unable to open incident");
    }
  };

  const restart = async () => {
    setBusy(true);
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
    setError(null);
    try {
      const response = await jsonRequest<{ incident_id?: string }>(
        "/api/scenario/trigger-p204",
        { method: "POST" }
      );
      if (response.incident_id) await openIncident(response.incident_id);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Unable to trigger excursion");
    } finally {
      setBusy(false);
    }
  };

  const runAction = async (action: RuntimeAction) => {
    if (action.href) {
      window.open(action.href, "_blank", "noopener,noreferrer");
    }
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

  const normalCount = snapshot.pumps.filter((pump) => pump.state === "normal").length;
  const openAlerts = snapshot.alerts.filter((alert) => alert.status !== "resolved").length;

  return (
    <main className="command-center">
      <header className="command-header">
        <div className="brand-lockup">
          <div className="brand-glyph"><span /></div>
          <div>
            <p>Databricks Intelligence Platform</p>
            <h1>Asset Reliability Command Center</h1>
          </div>
        </div>
        <div className="platform-status">
          <div className={`connection-dot connection-${snapshot.platform.connection}`} />
          <div><strong>{snapshot.platform.source_label}</strong><span>Live operational state</span></div>
          <div className="latency"><strong>{snapshot.platform.latency_ms} ms</strong><span>Lakebase query latency</span></div>
        </div>
        <div className="header-actions">
          <button className="button-ghost" onClick={() => void restart()} disabled={busy}>Clear & restart</button>
          <button
            className="button-primary"
            onClick={() => void trigger()}
            disabled={busy || snapshot.scenario.p204_triggered}
          >Trigger P-204 excursion</button>
        </div>
      </header>

      {error && <div className="global-error">{error}</div>}

      <section className="summary-strip">
        <div><span>Fleet</span><strong>8 pumps</strong><small>Continuous telemetry</small></div>
        <div><span>Normal</span><strong>{normalCount}</strong><small>Inside operating envelope</small></div>
        <div><span>Agents active</span><strong>{snapshot.active_runs.length}</strong><small>Concurrent evaluations</small></div>
        <div><span>Open alerts</span><strong>{openAlerts}</strong><small>Current scenario</small></div>
        <div className="scenario-id"><span>Scenario</span><strong>{snapshot.scenario.id.replace("scenario_", "#")}</strong><small>Started {timeLabel(snapshot.scenario.started_at)}</small></div>
      </section>

      <section className="fleet-section">
        <div className="section-heading">
          <div><p>Live fleet</p><h2>Comparable pump assets</h2></div>
          <span className="streaming-label"><i /> Streaming now</span>
        </div>
        <div className="pump-grid">
          {snapshot.pumps.map((pump) => {
            const active = activeByAsset.get(pump.asset_id);
            const relatedAlert = snapshot.alerts.find((alert) => alert.asset_id === pump.asset_id);
            return (
              <article
                key={pump.asset_id}
                className={`pump-card pump-${pump.state} ${relatedAlert ? "pump-clickable" : ""}`}
                onClick={() => relatedAlert && void openIncident(relatedAlert.incident_id)}
              >
                <div className="pump-card-head">
                  <div><strong>{pump.asset_id}</strong><span>{pump.name}</span></div>
                  <StatusPill status={pump.state} label={pump.state_label} />
                </div>
                <div className="pump-signal">
                  <div><span>Vibration</span><strong>{pump.vibration_mm_s.toFixed(2)} <small>mm/s</small></strong></div>
                  <Sparkline pump={pump} />
                </div>
                <div className="pump-metrics">
                  <span><small>Flow</small>{pump.flow_m3_h.toFixed(1)} m³/h</span>
                  <span><small>Current</small>{pump.current_a.toFixed(1)} A</span>
                  <span><small>Threshold</small>{pump.warning_threshold_mm_s.toFixed(1)} mm/s</span>
                </div>
                {active && (
                  <div className="agent-active">
                    <div className="agent-orbit"><i /></div>
                    <div><strong>{active.label}</strong><span>{active.current_activity}</span></div>
                  </div>
                )}
                {pump.maintenance_label && <div className="maintenance-tag">✓ {pump.maintenance_label}</div>}
              </article>
            );
          })}
        </div>
      </section>

      <section className="history-grid">
        <article className="history-panel">
          <div className="history-heading"><div><p>Detected conditions</p><h2>Alert history</h2></div><span>{snapshot.alerts.length}</span></div>
          <div className="history-list">
            {!snapshot.alerts.length && <div className="empty-row">Threshold events will appear here.</div>}
            {snapshot.alerts.map((alert) => (
              <button className="history-row" key={alert.id} onClick={() => void openIncident(alert.incident_id)}>
                <span className={`row-marker marker-${alert.status}`} />
                <div className="row-main"><strong>{alert.asset_id} · {alert.title}</strong><span>{alert.resolution ?? alert.condition}</span></div>
                <div className="row-end"><StatusPill status={alert.status} label={alert.status_label} /><time>{timeLabel(alert.observed_at)}</time></div>
                <span className="row-chevron">›</span>
              </button>
            ))}
          </div>
        </article>

        <article className="history-panel">
          <div className="history-heading"><div><p>Autonomous operations</p><h2>Invocation history</h2></div><span>{snapshot.invocations.length}</span></div>
          <div className="history-list">
            {!snapshot.invocations.length && <div className="empty-row">Agent invocations will appear here.</div>}
            {snapshot.invocations.map((run) => (
              <button className="history-row" key={run.id} onClick={() => void openIncident(run.incident_id)}>
                <span className={`run-symbol run-${run.status}`}><i /></span>
                <div className="row-main"><strong>{run.asset_id} · {run.title}</strong><span>{run.outcome ?? "Reviewing live operating context"}</span></div>
                <div className="row-end"><StatusPill status={run.status} label={run.status_label} /><time>{timeLabel(run.started_at)}</time></div>
                <span className="row-chevron">›</span>
              </button>
            ))}
          </div>
        </article>
      </section>

      {selectedIncidentId && (
        <div className="overlay" role="dialog" aria-modal="true" aria-label="Incident review">
          <div className="overlay-backdrop" onClick={() => setSelectedIncidentId(null)} />
          <section className="incident-review">
            {!incident ? (
              <div className="incident-loading">Loading incident review…</div>
            ) : (
              <>
                <header className="incident-header">
                  <div><p>Incident review · {incident.asset_id}</p><h2>{incident.title}</h2><span>{incident.condition}</span></div>
                  <StatusPill status={incident.status} label={incident.status_label} />
                  <button className="close-button" onClick={() => setSelectedIncidentId(null)} aria-label="Close incident review">×</button>
                </header>
                <div className="incident-body">
                  <section className="narrative-pane">
                    <div className="pane-heading"><p>Agent assessment</p><h3>What the agent found</h3></div>
                    <div className="narrative-card">
                      {incident.narrative.map((paragraph, index) => <p key={index}>{paragraph}</p>)}
                    </div>
                    {incident.conversation.length > 0 && (
                      <div className="conversation-thread">
                        {incident.conversation.map((turn) => (
                          <div className={`conversation-turn turn-${turn.role}`} key={turn.id}>
                            <span>{turn.role === "operator" ? "You" : "Reliability agent"}</span>
                            <p>{turn.content}</p>
                          </div>
                        ))}
                      </div>
                    )}
                    <form className="follow-up" onSubmit={submitFollowUp}>
                      <label htmlFor="follow-up">Ask a follow-up</label>
                      <div><input id="follow-up" value={followUp} onChange={(event) => setFollowUp(event.target.value)} placeholder="Ask about evidence, risk, or the response…" disabled={busy} /><button disabled={busy || !followUp.trim()}>Send</button></div>
                    </form>
                  </section>

                  <section className="activity-pane">
                    <div className="pane-heading"><p>Runtime evidence</p><h3>Agent activity</h3><span>Events appear exactly as emitted</span></div>
                    <div className="timeline" ref={timelineRef}>
                      {!incident.activities.length && <div className="timeline-empty"><i />Waiting for the first runtime event…</div>}
                      {incident.activities.map((activity) => (
                        <article className={`timeline-event event-${activity.status}`} key={activity.id}>
                          <div className="timeline-rail"><span /><i /></div>
                          <div className="event-content">
                            <div className="event-kicker"><span>{activity.category}</span><time>{timeLabel(activity.emitted_at)}</time></div>
                            <div className="event-title"><div><strong>{activity.title}</strong>{activity.actor && <span>{activity.actor}</span>}</div><StatusPill status={activity.status} label={activity.status_label} /></div>
                            {activity.summary && <p>{activity.summary}</p>}
                            {activity.details.length > 0 && <dl>{activity.details.map((detail) => <div key={`${activity.id}-${detail.label}`}><dt>{detail.label}</dt><dd>{detail.value}</dd></div>)}</dl>}
                            {activity.action && <button className="runtime-action" disabled={busy} onClick={() => void runAction(activity.action!)}>{activity.action.label}</button>}
                          </div>
                        </article>
                      ))}
                    </div>
                  </section>
                </div>
                <footer className="incident-footer">
                  <div><span>Total tokens</span><strong>{(incident.totals.input_tokens + incident.totals.output_tokens).toLocaleString()}</strong></div>
                  <div><span>Estimated cost</span><strong>${incident.totals.estimated_cost_usd.toFixed(4)}</strong></div>
                  <div><span>Tool calls</span><strong>{incident.totals.tool_calls}</strong></div>
                  <button onClick={() => setSelectedIncidentId(null)}>Close review</button>
                </footer>
              </>
            )}
          </section>
        </div>
      )}
    </main>
  );
}

export default App;
