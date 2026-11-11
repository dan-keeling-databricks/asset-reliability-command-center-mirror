import { FormEvent, ReactNode, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import { GenieDrawer } from "./GenieDrawer";
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

function MarkdownContent({ content }: { content: string }) {
  const fixedHeading = ({ children }: { children?: ReactNode }) => <p className="markdown-heading"><strong>{children}</strong></p>;
  return (
    <Markdown
      remarkPlugins={[remarkGfm]}
      components={{
        a: ({ children, ...props }) => <a {...props} target="_blank" rel="noopener noreferrer">{children}</a>,
        h1: fixedHeading,
        h2: fixedHeading,
        h3: fixedHeading,
        h4: fixedHeading,
        h5: fixedHeading,
        h6: fixedHeading
      }}
    >
      {content}
    </Markdown>
  );
}

function AgentMessage({ content }: { content: string }) {
  const fields = content.split("\n").map((line) =>
    line.match(/^-?\s*\*\*(Finding|Evidence|Risk|Action):\*\*\s*(.*)$/)
  );
  const isTerminalResponse = fields.length === 4
    && fields.every((field, index) => field?.[1] === ["Finding", "Evidence", "Risk", "Action"][index]);

  if (!isTerminalResponse) {
    return (
      <div className="markdown-message">
        <MarkdownContent content={content} />
      </div>
    );
  }

  return (
    <div className="structured-message">
      {fields.map((field) => <div className="structured-field" key={field![1]}><strong>{field![1]}</strong><div className="structured-value"><MarkdownContent content={field![2]} /></div></div>)}
    </div>
  );
}

type HistoryRecord = {
  alert: AlertSummary;
  invocation?: InvocationSummary;
};

type ConversationItem =
  | { kind: "activity"; at: string; activity: RuntimeActivity }
  | { kind: "message"; at: string; turn: IncidentDetail["conversation"][number] };

function conversationItemId(item: ConversationItem) {
  return `${item.kind}-${item.kind === "activity" ? item.activity.id : item.turn.id}`;
}

type LatencyMetrics = Pick<
  DashboardSnapshot["platform"],
  "kafka_to_lakebase_latency_ms" | "lakebase_to_ui_latency_ms" | "end_to_end_latency_ms"
>;

function latencyMetrics(snapshot: DashboardSnapshot): LatencyMetrics {
  return {
    kafka_to_lakebase_latency_ms: snapshot.platform.kafka_to_lakebase_latency_ms,
    lakebase_to_ui_latency_ms: snapshot.platform.lakebase_to_ui_latency_ms,
    end_to_end_latency_ms: snapshot.platform.end_to_end_latency_ms
  };
}

function architectureAssetUrl(cacheKey: string) {
  return `/architecture-tab.html?build=${encodeURIComponent(cacheKey)}#embed`;
}

function App() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [incident, setIncident] = useState<IncidentDetail | null>(null);
  const [selectedIncidentId, setSelectedIncidentId] = useState<string | null>(null);
  const [activeView, setActiveView] = useState<"fleet" | "history" | "architecture">("fleet");
  const [incidentView, setIncidentView] = useState<"conversation" | "timeline" | "cost">("conversation");
  const [menuOpen, setMenuOpen] = useState(false);
  const [genieOpen, setGenieOpen] = useState(false);
  const [followUp, setFollowUp] = useState("");
  const [followUpPending, setFollowUpPending] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [liveUpdateWarning, setLiveUpdateWarning] = useState(false);
  const [selectedOperator, setSelectedOperator] = useState("");
  const [displayedLatencies, setDisplayedLatencies] = useState<LatencyMetrics | null>(null);
  const [fontScale, setFontScale] = useState(() => {
    const saved = Number(localStorage.getItem("command-center-font-scale"));
    return [100, 110, 120, 130].includes(saved) ? saved : 100;
  });
  const conversationRef = useRef<HTMLDivElement>(null);
  const conversationIncidentRef = useRef<string | null>(null);
  const conversationAtBottomRef = useRef(false);
  const latestLatenciesRef = useRef<LatencyMetrics | null>(null);
  const reconnectTimerRef = useRef<number | null>(null);
  const visibleConversationIdsRef = useRef(new Set<string>());
  const [visibleConversationIds, setVisibleConversationIds] = useState(new Set<string>());
  const [pendingConversationIds, setPendingConversationIds] = useState<string[]>([]);
  const [enteringConversationId, setEnteringConversationId] = useState<string | null>(null);

  const conversationItems = useMemo<ConversationItem[]>(() => incident ? [
    ...incident.activities.map((activity) => ({ kind: "activity" as const, at: activity.emitted_at, activity })),
    ...incident.conversation.map((turn) => ({ kind: "message" as const, at: turn.created_at, turn }))
  ].sort((a, b) => new Date(a.at).getTime() - new Date(b.at).getTime()) : [], [incident]);

  const refreshIncident = useCallback(async (incidentId: string) => {
    const detail = await jsonRequest<IncidentDetail>(`/api/incidents/${incidentId}`);
    setIncident(detail);
  }, []);

  useEffect(() => {
    document.documentElement.style.setProperty("--font-scale", String(fontScale / 100));
    localStorage.setItem("command-center-font-scale", String(fontScale));
  }, [fontScale]);

  useEffect(() => {
    if (!snapshot) return;
    const latest = latencyMetrics(snapshot);
    latestLatenciesRef.current = latest;
    setDisplayedLatencies((current) => current ?? latest);
  }, [
    snapshot?.platform.kafka_to_lakebase_latency_ms,
    snapshot?.platform.lakebase_to_ui_latency_ms,
    snapshot?.platform.end_to_end_latency_ms
  ]);

  // Keep the operational snapshot live while making the headline latency display readable.
  useEffect(() => {
    const timer = window.setInterval(() => {
      if (latestLatenciesRef.current) setDisplayedLatencies(latestLatenciesRef.current);
    }, 3000);
    return () => window.clearInterval(timer);
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
        const markConnected = () => {
          if (reconnectTimerRef.current !== null) {
            window.clearTimeout(reconnectTimerRef.current);
            reconnectTimerRef.current = null;
          }
          setLiveUpdateWarning(false);
        };
        source.onopen = markConnected;
        source.onmessage = (event) => {
          setSnapshot(JSON.parse(event.data) as DashboardSnapshot);
          markConnected();
        };
        source.onerror = () => {
          if (reconnectTimerRef.current === null) {
            reconnectTimerRef.current = window.setTimeout(() => {
              setLiveUpdateWarning(true);
              reconnectTimerRef.current = null;
            }, 10_000);
          }
        };
      } catch (caught) {
        setError(caught instanceof Error ? caught.message : "Unable to load command centre");
      }
    })();
    return () => {
      disposed = true;
      if (reconnectTimerRef.current !== null) window.clearTimeout(reconnectTimerRef.current);
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
    if (!incident) {
      conversationIncidentRef.current = null;
      visibleConversationIdsRef.current = new Set();
      setVisibleConversationIds(new Set());
      setPendingConversationIds([]);
      return;
    }
    const itemIds = conversationItems.map(conversationItemId);
    if (conversationIncidentRef.current !== incident.id) {
      conversationIncidentRef.current = incident.id;
      const initialIds = new Set(itemIds);
      visibleConversationIdsRef.current = initialIds;
      setVisibleConversationIds(initialIds);
      setPendingConversationIds([]);
      setEnteringConversationId(null);
      window.requestAnimationFrame(() => {
        const conversation = conversationRef.current;
        if (!conversation) return;
        conversation.scrollTop = 0;
        conversationAtBottomRef.current = (
          conversation.scrollHeight - conversation.clientHeight <= 24
        );
      });
      return;
    }
    setPendingConversationIds((current) => {
      const queued = new Set(current);
      const additions = itemIds.filter(
        (id) => !visibleConversationIdsRef.current.has(id) && !queued.has(id)
      );
      return additions.length ? [...current, ...additions] : current;
    });
  }, [conversationItems, incident]);

  useEffect(() => {
    const nextId = pendingConversationIds[0];
    if (!nextId) return;
    if (!visibleConversationIdsRef.current.has(nextId)) {
      const nextVisible = new Set(visibleConversationIdsRef.current);
      nextVisible.add(nextId);
      visibleConversationIdsRef.current = nextVisible;
      setVisibleConversationIds(nextVisible);
    }
    setEnteringConversationId(nextId);
    if (conversationAtBottomRef.current) {
      window.requestAnimationFrame(() => {
        const conversation = conversationRef.current;
        conversation?.scrollTo({ top: conversation.scrollHeight, behavior: "smooth" });
      });
    }
    const timer = window.setTimeout(() => {
      setEnteringConversationId(null);
      setPendingConversationIds((current) => current.slice(1));
    }, 1000);
    return () => window.clearTimeout(timer);
  }, [pendingConversationIds[0]]);

  useEffect(() => {
    const operatorEmail = snapshot?.shift_operator.operator?.email;
    if (operatorEmail) setSelectedOperator(operatorEmail);
  }, [snapshot?.shift_operator.operator?.email]);

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

  const handover = async () => {
    const selected = snapshot?.shift_operator.recent_users.find(
      (user) => user.email === selectedOperator
    );
    if (!snapshot || !selected) return;
    const confirmed = window.confirm(
      `Hand over control to ${selected.name} (${selected.email})? ` +
      "Active reviews will be archived and a fresh scenario will start for everyone."
    );
    if (!confirmed) return;
    setBusy(true);
    setError(null);
    try {
      await jsonRequest("/api/operator/handover", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          operator_email: selected.email,
          expected_version: snapshot.shift_operator.version,
          confirmed: true
        })
      });
      setMenuOpen(false);
      setIncident(null);
      setSelectedIncidentId(null);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : "Unable to hand over control");
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
    if (!selectedIncidentId || !followUp.trim() || !isOperator || followUpPending) return;
    setBusy(true);
    setFollowUpPending(true);
    setError(null);
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
      setFollowUpPending(false);
      setBusy(false);
    }
  };

  if (!snapshot) {
    return <div className="loading-screen"><div className="loading-mark" />Connecting to live operations…</div>;
  }

  const isOperator = snapshot.viewer?.is_operator ?? false;
  const operator = snapshot.shift_operator.operator;
  const latencies = displayedLatencies ?? latencyMetrics(snapshot);

  return (
    <main className="command-center">
      <header className="command-header">
        <div className="brand-lockup"><div className="brand-glyph"><span /></div><div><p>Databricks Intelligence Platform</p><h1>Asset Reliability Command Centre</h1></div></div>
        <div className="latency-panel">
          <div className={`connection-dot connection-${snapshot.platform.connection}`} aria-label={snapshot.platform.connection} />
          <div><span>Kafka → Lakebase</span><strong>{latencies.kafka_to_lakebase_latency_ms} ms</strong></div>
          <div><span>Lakebase → UI</span><strong>{latencies.lakebase_to_ui_latency_ms} ms</strong></div>
          <div className="latency-total"><span>End to end</span><strong>{latencies.end_to_end_latency_ms} ms</strong></div>
        </div>
        <button className="genie-launch" type="button" onClick={() => setGenieOpen(true)}><span>✦</span><div><strong>Ask fleet telemetry</strong><small>Powered by Genie</small></div></button>
        <div className="demo-menu-wrap">
          <button className="menu-button" aria-label="Open demo controls" aria-expanded={menuOpen} onClick={() => setMenuOpen(!menuOpen)}><i /><i /><i /></button>
          {menuOpen && <div className="demo-menu"><p>Shift operator</p><div className="operator-control"><label htmlFor="shift-operator">Current control</label><select id="shift-operator" value={selectedOperator} onChange={(event) => setSelectedOperator(event.target.value)}>{snapshot.shift_operator.recent_users.map((user) => <option value={user.email} key={user.email}>{user.name} · {user.email}</option>)}</select><button onClick={() => void handover()} disabled={busy || !selectedOperator || selectedOperator === operator?.email}>Hand over control<span>Archives active reviews and starts a fresh scenario</span></button></div><p>Presentation</p><div className="text-size-control"><span>Text size</span><button aria-label="Decrease text size" onClick={() => setFontScale((value) => Math.max(100, value - 10))} disabled={fontScale === 100}>−</button><strong>{fontScale}%</strong><button aria-label="Increase text size" onClick={() => setFontScale((value) => Math.min(130, value + 10))} disabled={fontScale === 130}>+</button></div><p>Demo controls</p><button onClick={() => void trigger()} disabled={busy || !isOperator || snapshot.scenario.p204_triggered}>Trigger P-204 excursion<span>{isOperator ? "Start the governed failure scenario" : "Available to the shift operator"}</span></button><button className="menu-danger" onClick={() => void restart()} disabled={busy || !isOperator}>Clear & restart<span>{isOperator ? "Reset all scenario activity" : "Available to the shift operator"}</span></button></div>}
        </div>
      </header>

      {!isOperator && operator && <div className="read-only-banner" role="status"><strong>Read-only</strong><span>{operator.name} ({operator.email}) is the current shift operator. You can hand over control from the menu.</span></div>}

      {error && <div className="global-error">{error}</div>}
      {liveUpdateWarning && <div className="global-error" role="status">Live updates are reconnecting</div>}

      <nav className="primary-tabs" aria-label="Command centre views">
        <button className={activeView === "fleet" ? "active" : ""} onClick={() => setActiveView("fleet")}>Live fleet</button>
        <button className={activeView === "history" ? "active" : ""} onClick={() => setActiveView("history")}>History <span>{snapshot.alerts.length}</span></button>
        <button className={activeView === "architecture" ? "active" : ""} onClick={() => setActiveView("architecture")}>Architecture</button>
      </nav>

      {activeView === "fleet" ? (
        <section className="fleet-section">
          <div className="section-heading"><div><p>Live fleet</p><h2>Circulatory pump assets</h2></div><span className="streaming-label"><i /> Streaming now</span></div>
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
      ) : activeView === "history" ? (
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
      ) : <ArchitectureView fontScale={fontScale} />}

      {selectedIncidentId && <div className="overlay" role="dialog" aria-modal="true" aria-label="Incident review">
        <div className="overlay-backdrop" onClick={() => setSelectedIncidentId(null)} />
        <section className="incident-review">
          {!incident ? <div className="incident-loading">Loading incident review…</div> : <>
            <header className="incident-header"><div><p>Incident review · {incident.asset_id}</p><h2>{incident.title}</h2></div><div className="view-toggle"><button className={incidentView === "conversation" ? "active" : ""} onClick={() => setIncidentView("conversation")}>Conversation</button><button className={incidentView === "timeline" ? "active" : ""} onClick={() => setIncidentView("timeline")}>Execution timeline</button><button className={incidentView === "cost" ? "active" : ""} onClick={() => setIncidentView("cost")}>Cost</button></div><StatusPill status={incident.status} label={incident.status_label} /><button className="close-button" onClick={() => setSelectedIncidentId(null)} aria-label="Close incident review">×</button></header>
            <div className="incident-workspace">
              <section className="initiating-prompt"><div className="prompt-icon">↗</div><div><strong>Initiating prompt</strong><p>{incident.trigger_prompt}</p></div></section>
              {incidentView === "conversation" ? <div className="unified-conversation" ref={conversationRef} onScroll={(event) => {
                const target = event.currentTarget;
                conversationAtBottomRef.current = target.scrollHeight - target.scrollTop - target.clientHeight <= 24;
              }}>
                {!conversationItems.length && <div className="timeline-empty"><i />Waiting for the first runtime event…</div>}
                {conversationItems.filter((item) => (
                  conversationIncidentRef.current !== incident.id
                  || visibleConversationIds.has(conversationItemId(item))
                )).map((item) => {
                  const itemId = conversationItemId(item);
                  const entering = itemId === enteringConversationId;
                  if (item.kind === "activity") {
                    return <ActivityCard key={item.activity.id} activity={item.activity} completion={completionFor(item.activity, incident.activities)} busy={busy} runAction={runAction} entering={entering} />;
                  }
                  return <article className={`chat-event chat-${item.turn.role} ${entering ? "conversation-entering" : ""}`} key={item.turn.id}><EventIcon category={item.turn.role} /><div><div className="chat-meta"><strong>{item.turn.role === "operator" ? "You" : "Final response"}</strong><time>{timeLabel(item.turn.created_at)}</time></div>{item.turn.role === "agent" ? <AgentMessage content={item.turn.content} /> : <p>{item.turn.content}</p>}</div></article>;
                })}
                {followUpPending && <div className="follow-up-status-row"><div aria-hidden="true" /><div className="agent-active follow-up-status" role="status" aria-live="polite"><div className="agent-orbit" aria-hidden="true"><i /></div><div><strong>Follow-up in progress</strong><span>Agent is working on a response</span></div></div></div>}
              </div> : incidentView === "timeline" ? <ExecutionTimeline activities={automatedActivities(incident)} /> : <CostView incident={incident} />}
              <form className="follow-up" onSubmit={submitFollowUp}><label htmlFor="follow-up">Ask a follow-up</label><div><input id="follow-up" value={followUp} onChange={(event) => setFollowUp(event.target.value)} placeholder={isOperator ? "Ask about evidence, risk, or the response…" : "Only the current operator can ask follow-ups"} disabled={!isOperator || busy || followUpPending} /><button disabled={!isOperator || busy || followUpPending || !followUp.trim()}>Send</button></div></form>
            </div>
            <footer className="incident-footer"><div><span>Total tokens</span><strong>{(incident.totals.input_tokens + incident.totals.output_tokens).toLocaleString()}</strong></div><div><span>Estimated cost</span><strong>${incident.totals.estimated_cost_usd.toFixed(4)}</strong></div><div><span>Tool calls</span><strong>{incident.totals.tool_calls}</strong></div><button onClick={() => setSelectedIncidentId(null)}>Close review</button></footer>
          </>}
        </section>
      </div>}
      <GenieDrawer open={genieOpen} onClose={() => setGenieOpen(false)} />
    </main>
  );
}

function ArchitectureView({ fontScale }: { fontScale: number }) {
  const frameRef = useRef<HTMLIFrameElement>(null);
  const [architectureSource, setArchitectureSource] = useState<string | null>(null);
  const applyFontScale = useCallback(() => {
    frameRef.current?.contentDocument?.documentElement.style.setProperty("--font-scale", String(fontScale / 100));
  }, [fontScale]);

  useEffect(() => {
    let disposed = false;
    void jsonRequest<{ git_commit: string }>("/api/build-info", { cache: "no-store" })
      .then((buildInfo) => {
        const cacheKey = buildInfo.git_commit.trim() || `session-${Date.now()}`;
        if (!disposed) setArchitectureSource(architectureAssetUrl(cacheKey));
      })
      .catch(() => {
        if (!disposed) setArchitectureSource(architectureAssetUrl(`session-${Date.now()}`));
      });
    return () => { disposed = true; };
  }, []);

  useEffect(() => applyFontScale(), [applyFontScale]);

  return <section className="architecture-page" aria-label="Solution architecture">
    <h2 className="architecture-title">A real-time Databricks architecture for detecting asset <strong>risk</strong>, governing autonomous <strong>response</strong>, and preserving operational <strong>evidence</strong>.</h2>
    {architectureSource
      ? <iframe ref={frameRef} className="architecture-frame" src={architectureSource} title="Asset Reliability Command Centre architecture" onLoad={applyFontScale} />
      : <div className="architecture-frame architecture-frame-loading" role="status">Loading architecture…</div>}
  </section>;
}

function CostView({ incident }: { incident: IncidentDetail }) {
  const totalCost = incident.inferences.reduce((sum, inference) => sum + inference.estimated_cost_usd, 0);
  const totalTokens = incident.inferences.reduce((sum, inference) => sum + inference.total_tokens, 0);
  const modelsUsed = Array.from(new Set(incident.inferences.map((inference) => inference.model)));
  return <div className="cost-view">
    <section className="cost-summary"><div><span>Estimated inference cost</span><strong>${totalCost.toFixed(6)}</strong></div><div><span>LLM calls</span><strong>{incident.inferences.length}</strong></div><div><span>Total tokens</span><strong>{totalTokens.toLocaleString()}</strong></div><div><span>Model used</span><strong className="cost-models">{modelsUsed.length ? modelsUsed.join(", ") : "—"}</strong></div></section>
    <div className="cost-table-wrap"><table className="cost-table"><thead><tr><th>Time</th><th>Purpose</th><th>Model</th><th>Input</th><th>Output</th><th>Total</th><th>Estimated cost</th><th>Trace</th></tr></thead><tbody>{incident.inferences.map((inference) => <tr key={inference.id}><td>{timeLabel(inference.occurred_at)}</td><td>{inference.purpose}</td><td><code>{inference.model}</code></td><td>{inference.input_tokens.toLocaleString()}</td><td>{inference.output_tokens.toLocaleString()}</td><td>{inference.total_tokens.toLocaleString()}</td><td>${inference.estimated_cost_usd.toFixed(6)}</td><td><a href={inference.trace_url ?? `#mlflow-trace-${inference.id}`} target={inference.trace_url ? "_blank" : undefined} rel={inference.trace_url ? "noopener noreferrer" : undefined} onClick={inference.trace_url ? undefined : (event) => event.preventDefault()}>MLflow trace <span className="trace-link-icon" aria-hidden="true">↗</span></a></td></tr>)}</tbody></table></div>
    {!incident.inferences.length && <div className="timeline-empty">No LLM inference has been recorded for this incident.</div>}
  </div>;
}

function automatedActivities(incident: IncidentDetail) {
  const firstFollowUp = incident.conversation
    .filter((turn) => turn.role === "operator")
    .map((turn) => new Date(turn.created_at).getTime())
    .sort((a, b) => a - b)[0];
  if (!firstFollowUp) return incident.activities;
  return incident.activities.filter(
    (activity) => new Date(activity.emitted_at).getTime() < firstFollowUp
  );
}

function completionFor(activity: RuntimeActivity, activities: RuntimeActivity[]) {
  if (activity.status !== "waiting" || activity.category !== "tool_call") return undefined;
  const activityTime = new Date(activity.emitted_at).getTime();
  return activities.find((candidate) => (
    candidate.category === "tool_result"
    && candidate.title === activity.title
    && candidate.status === "complete"
    && new Date(candidate.emitted_at).getTime() >= activityTime
  ));
}

function executionTimelineLabel(activity: RuntimeActivity) {
  if (activity.category !== "skill") return activity.title;
  const components = activity.title.split(".");
  return components[components.length - 1] || activity.title;
}

function gatewayDecision(activity: RuntimeActivity) {
  const decision = activity.details.find((detail) => detail.label.toLowerCase() === "decision")?.value;
  return decision && ["ALLOW", "ASK", "DENY"].includes(decision) ? decision : null;
}

function ActivityDetails({ activity, details }: { activity: RuntimeActivity; details: RuntimeActivity["details"] }) {
  return <dl>{details.map((detail) => {
    const isMcpToolInventory = activity.category === "mcp" && detail.label.toLowerCase() === "available";
    if (!isMcpToolInventory) {
      return <div key={`${activity.id}-${detail.label}`}><dt>{detail.label}</dt><dd>{detail.value}</dd></div>;
    }

    const tools = detail.value.split("\n").map((tool) => tool.trim()).filter(Boolean);
    const splitAt = Math.ceil(tools.length / 2);
    const columns = [tools.slice(0, splitAt), tools.slice(splitAt)].filter((column) => column.length > 0);
    return <div className="mcp-tool-detail" key={`${activity.id}-${detail.label}`}><dt>{detail.label}</dt><dd className="mcp-tool-columns">{columns.map((column, index) => <ul key={`${activity.id}-${detail.label}-${index}`}>{column.map((tool) => <li key={tool}>{tool}</li>)}</ul>)}</dd></div>;
  })}</dl>;
}

function activityPresentation(activity: RuntimeActivity) {
  if (activity.category === "skill") {
    return {
      typeLabel: "SKILL",
      title: "Loaded the governed response procedure",
      subtitle: null,
      showSummary: false
    };
  }
  if (activity.category === "mcp") {
    return {
      typeLabel: "MCP",
      title: "Discovering governed maintenance capabilities",
      subtitle: null,
      showSummary: true
    };
  }
  if (activity.category === "model_call") {
    return {
      typeLabel: "Model Execution",
      title: null,
      subtitle: activity.actor,
      showSummary: true
    };
  }
  if (activity.category === "tool_call") {
    return {
      typeLabel: "Tool Call",
      title: `Called ${activity.title}`,
      subtitle: null,
      showSummary: true
    };
  }
  return {
    typeLabel: activity.category.replace("_", " "),
    title: activity.title,
    subtitle: activity.actor,
    showSummary: true
  };
}

function ActivityCard({ activity, completion, busy, runAction, compact = false, entering = false }: { activity: RuntimeActivity; completion?: RuntimeActivity; busy: boolean; runAction: (action: RuntimeAction) => Promise<void>; compact?: boolean; entering?: boolean }) {
  const completedApproval = Boolean(completion && activity.status === "waiting");
  const displayedActivity: RuntimeActivity = completedApproval ? {
    ...activity,
    status: "complete",
    status_label: "Completed",
    summary: completion?.summary ?? activity.summary,
    details: [
      ...activity.details.filter((detail) => !["decision", "execution"].includes(detail.label.toLowerCase())),
      { label: "Decision", value: "ALLOW" },
      { label: "Execution", value: "Executed after approval" }
    ],
    action: activity.action?.href ? {
      label: "View approval record",
      kind: "link",
      href: activity.action.href
    } : null,
    completed_at: completion?.completed_at ?? completion?.emitted_at
  } : activity;
  const category = displayedActivity.status === "waiting" ? "approval" : displayedActivity.category;
  const policy = gatewayDecision(displayedActivity);
  const runtimeLabel = { running: "Running", complete: "Completed", waiting: "Waiting", failed: "Failed", info: "Information" }[displayedActivity.status];
  const contextLabels = new Set(["finding", "evidence", "risk", "action"]);
  const approvalContext = displayedActivity.details.filter((detail) => contextLabels.has(detail.label.toLowerCase()));
  const isSubmitWorkOrder = displayedActivity.category === "tool_call"
    && displayedActivity.title.trim().toLowerCase().replace(/[\s-]+/g, "_") === "submit_work_order";
  const details = displayedActivity.details.filter((detail) => ![
    "decision", "execution", "input tokens", "output tokens", "model output", "purpose",
    "estimated cost", "trace url", ...contextLabels,
    ...(isSubmitWorkOrder ? ["rationale", "planned window"] : [])
  ].includes(detail.label.toLowerCase()));
  const tokenTotal = displayedActivity.category === "model_call" ? displayedActivity.details.filter((detail) => /tokens/i.test(detail.label)).reduce((sum, detail) => sum + Number(detail.value.replaceAll(",", "")), 0) : 0;
  const presentation = activityPresentation(displayedActivity);
  const skillProcedureHref = displayedActivity.category === "skill" ? displayedActivity.action?.href ?? "/api/governed-response-procedure" : null;
  const reviewAction = displayedActivity.category !== "skill" && displayedActivity.action?.href ? { ...displayedActivity.action, endpoint: undefined } : null;
  const continueAction = displayedActivity.action?.endpoint ? { ...displayedActivity.action, href: undefined } : null;
  const actions = displayedActivity.action && <div className="runtime-actions">{reviewAction && <button className="runtime-action" disabled={busy} onClick={() => void runAction(reviewAction)}>{reviewAction.label}</button>}{continueAction && <button className="runtime-action runtime-action-primary" disabled={busy} onClick={() => void runAction(continueAction)}>I've approved, continue</button>}</div>;
  const approvalRecordUnavailable = completedApproval && !displayedActivity.action?.href;
  return <article className={`chat-event runtime-event event-${displayedActivity.status} ${compact ? "compact-card" : ""} ${entering ? "conversation-entering" : ""}`}><EventIcon category={category} /><div className="event-content"><div className="event-kicker">{presentation.typeLabel && <span>{presentation.typeLabel}</span>}<time>{timeLabel(displayedActivity.emitted_at)}</time></div><div className="event-title"><div>{presentation.title && <strong>{presentation.title}</strong>}{presentation.subtitle && <span className={!presentation.title ? "event-subtitle-primary" : undefined}>{presentation.subtitle}</span>}{skillProcedureHref && <a className="skill-procedure-link" href={skillProcedureHref} target="_blank" rel="noopener noreferrer">Read Standard Operating Procedure <span aria-hidden="true">↗</span></a>}</div><span className={`runtime-status runtime-status-${displayedActivity.status}`}>{runtimeLabel}</span></div>{presentation.showSummary && displayedActivity.summary && (displayedActivity.category === "model_call" ? <section className="event-output"><strong>Output</strong><AgentMessage content={displayedActivity.summary} /></section> : <p>{displayedActivity.summary}</p>)}{approvalContext.length > 0 && <section className="approval-context"><strong>Decision context</strong>{approvalContext.map((detail) => <div key={`${displayedActivity.id}-${detail.label}`}><span>{detail.label}</span><p>{detail.value}</p></div>)}</section>}{details.length > 0 && <ActivityDetails activity={displayedActivity} details={details} />}{policy && <section className={`gateway-policy policy-${policy.toLowerCase()}`}><strong>Unity Gateway Applied Service Policy</strong><span>{policy}</span><p>{displayedActivity.details.find((detail) => detail.label.toLowerCase() === "execution")?.value ?? (policy === "ALLOW" ? "Execution occurred" : "Execution did not occur")}</p>{actions}{approvalRecordUnavailable && <p className="approval-record-unavailable">Approval record link is not available for this runtime.</p>}</section>}{!policy && actions}<footer className="event-footer">{tokenTotal > 0 && <span>Total tokens <strong>{tokenTotal.toLocaleString()}</strong></span>}</footer></div></article>;
}

type TimelineHover = { activity: RuntimeActivity; left: number; top: number; width: number };

function GanttLane({ label, activities, start, range, setHovered, setPinned }: { label: string; activities: RuntimeActivity[]; start: number; range: number; setHovered: (hover: TimelineHover | null) => void; setPinned: (hover: TimelineHover) => void }) {
  const bars = useRef(new Map<string, HTMLDivElement>());
  const [rows, setRows] = useState<Record<string, number>>({});

  useLayoutEffect(() => {
    const occupiedUntil: number[] = [];
    const nextRows: Record<string, number> = {};
    const measured = activities.map((activity) => {
      const bar = bars.current.get(activity.id);
      const barRect = bar?.getBoundingClientRect();
      const labelRect = bar?.querySelector("i")?.getBoundingClientRect();
      return { activity, left: barRect?.left ?? 0, right: Math.max(barRect?.right ?? 0, labelRect?.right ?? 0) };
    }).sort((a, b) => a.left - b.left);
    for (const item of measured) {
      let row = occupiedUntil.findIndex((right) => right + 10 <= item.left);
      if (row === -1) row = occupiedUntil.length;
      occupiedUntil[row] = item.right;
      nextRows[item.activity.id] = row;
    }
    setRows((current) => JSON.stringify(current) === JSON.stringify(nextRows) ? current : nextRows);
  });

  const rowCount = Math.max(1, ...Object.values(rows).map((row) => row + 1));
  return <section className="gantt-lane"><h3>{label}</h3><div className="execution-track" style={{ height: `${Math.max(44, rowCount * 34 + 10)}px` }}>{activities.map((activity) => {
    const activityStart = new Date(activity.started_at ?? activity.emitted_at).getTime();
    const activityEnd = new Date(activity.completed_at ?? activity.emitted_at).getTime();
    const left = Math.min(88, ((activityStart - start) / range) * 88);
    const width = activity.status === "waiting" ? 3 : Math.max(2, Math.min(25, ((Math.max(activityEnd, activityStart + 250) - activityStart) / range) * 88));
    const showCard = (target: HTMLElement) => {
      const rect = target.getBoundingClientRect();
      const popoverWidth = Math.min(700, window.innerWidth - 48);
      const leftPosition = window.innerWidth - rect.right >= popoverWidth + 20 ? rect.right + 12 : Math.max(24, rect.left - popoverWidth - 12);
      setHovered({ activity, left: leftPosition, top: Math.max(24, Math.min(rect.top, window.innerHeight - 544)), width: popoverWidth });
    };
    const pinCard = (target: HTMLElement) => {
      showCard(target);
      const rect = target.getBoundingClientRect();
      const popoverWidth = Math.min(700, window.innerWidth - 48);
      const leftPosition = window.innerWidth - rect.right >= popoverWidth + 20 ? rect.right + 12 : Math.max(24, rect.left - popoverWidth - 12);
      setPinned({ activity, left: leftPosition, top: Math.max(24, Math.min(rect.top, window.innerHeight - 544)), width: popoverWidth });
    };
    return <div ref={(element) => { if (element) bars.current.set(activity.id, element); else bars.current.delete(activity.id); }} className={`execution-bar category-${activity.category} bar-${activity.status}`} key={activity.id} style={{ left: `${left}%`, width: `${width}%`, top: `${10 + (rows[activity.id] ?? 0) * 34}px` }} tabIndex={0} onMouseEnter={(event) => showCard(event.currentTarget)} onMouseLeave={() => setHovered(null)} onFocus={(event) => showCard(event.currentTarget)} onBlur={() => setHovered(null)} onClick={(event) => pinCard(event.currentTarget)}><i>{executionTimelineLabel(activity)}</i></div>;
  })}</div></section>;
}

function ExecutionTimeline({ activities }: { activities: RuntimeActivity[] }) {
  const [hovered, setHovered] = useState<TimelineHover | null>(null);
  const [pinned, setPinned] = useState<TimelineHover | null>(null);
  const visibleCard = pinned ?? hovered;
  useEffect(() => {
    if (!pinned) return;
    const dismiss = (event: MouseEvent) => {
      const target = event.target as Element;
      if (!target.closest(".gantt-popover") && !target.closest(".execution-bar")) setPinned(null);
    };
    const dismissWithKeyboard = (event: KeyboardEvent) => { if (event.key === "Escape") setPinned(null); };
    document.addEventListener("mousedown", dismiss);
    document.addEventListener("keydown", dismissWithKeyboard);
    return () => {
      document.removeEventListener("mousedown", dismiss);
      document.removeEventListener("keydown", dismissWithKeyboard);
    };
  }, [pinned]);
  const shown = activities.filter((activity) => ["model_call", "skill", "tool_call", "tool_result", "mcp"].includes(activity.category));
  const start = shown.length ? Math.min(...shown.map((activity) => new Date(activity.started_at ?? activity.emitted_at).getTime())) : 0;
  const end = shown.length ? Math.max(...shown.map((activity) => new Date(activity.completed_at ?? activity.emitted_at).getTime()), start + 1000) : start + 1000;
  const range = Math.max(end - start, 1000);
  const lanes = [{ label: "LLM calls", categories: ["model_call"] }, { label: "Skill reads", categories: ["skill"] }, { label: "Tool calls", categories: ["tool_call", "tool_result", "mcp"] }];
  return <div className="execution-view"><div className="execution-scale"><span>Start</span><span>Agent execution</span><span>Latest event</span></div>{shown.length === 0 ? <div className="timeline-empty">No runtime activity yet.</div> : lanes.map((lane) => <GanttLane key={lane.label} label={lane.label} activities={shown.filter((activity) => lane.categories.includes(activity.category))} start={start} range={range} setHovered={setHovered} setPinned={setPinned} />)}{visibleCard && <div className={`gantt-popover ${pinned ? "gantt-popover-pinned" : ""}`} style={{ left: visibleCard.left, top: visibleCard.top, width: visibleCard.width }}><ActivityCard activity={visibleCard.activity} completion={completionFor(visibleCard.activity, activities)} busy={false} runAction={async () => undefined} compact /></div>}</div>;
}

export default App;
