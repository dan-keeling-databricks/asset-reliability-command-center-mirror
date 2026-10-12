import { FormEvent, useEffect, useRef, useState } from "react";
import Markdown from "react-markdown";
import remarkGfm from "remark-gfm";
import type {
  GenieIdentity,
  GenieMessageView,
  GenieSubmission
} from "./genieTypes";

const STARTER_QUESTIONS = [
  "Which pumps recorded the highest vibration in the last 30 minutes?",
  "Compare average vibration and bearing temperature across the fleet today.",
  "Which pumps crossed 4.4 mm/s vibration in the last hour?"
];

type ConversationItem = {
  id: string;
  role: "user" | "genie";
  content: string;
  response?: GenieMessageView;
};

async function requestJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  if (!response.ok) {
    const body = await response.json().catch(() => ({ detail: "Request failed" }));
    throw new Error(body.detail ?? "Request failed");
  }
  return response.json() as Promise<T>;
}

function formatGeneratedAt(value?: string | null) {
  if (!value) return "Freshness reported when the answer completes";
  return `Answer generated ${new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }).format(new Date(value))}`;
}

function ResultTable({ response }: { response: GenieMessageView }) {
  if (!response.results.length) return null;
  return <div className="genie-results">
    {response.results.map((result) => <section key={result.attachment_id} className="genie-result-block">
      {!result.rows.length ? <div className="genie-empty-inline">The query completed but returned no telemetry rows.</div> : <div className="genie-table-wrap">
        <table>
          <thead><tr>{result.columns.map((column) => <th key={column}>{column}</th>)}</tr></thead>
          <tbody>{result.rows.map((row, rowIndex) => <tr key={`${result.attachment_id}-${rowIndex}`}>{row.map((value, columnIndex) => <td key={`${rowIndex}-${columnIndex}`}>{value ?? "—"}</td>)}</tr>)}</tbody>
        </table>
      </div>}
      {(result.truncated || (result.total_rows ?? 0) > result.rows.length) && <p className="genie-result-note">Showing the first result chunk{result.total_rows != null ? ` of ${result.total_rows.toLocaleString()} rows` : ""}.</p>}
    </section>)}
  </div>;
}

function GenieAnswer({ item, onSuggestion }: { item: ConversationItem; onSuggestion: (question: string) => void }) {
  const response = item.response;
  if (!response) return <div className="genie-progress" role="status"><i /><span>Submitting question…</span></div>;
  if (!response.terminal) return <div className="genie-progress" role="status"><i /><span>{response.status_label}</span></div>;
  if (response.error || response.status === "FAILED" || response.status === "CANCELLED") {
    return <div className="genie-answer-error" role="alert"><strong>Genie could not complete this question</strong><span>{response.error ?? "Try rephrasing the question or start a new conversation."}</span></div>;
  }
  const empty = !response.answer && !response.results.some((result) => result.rows.length);
  return <>
    {response.requires_clarification && <div className="genie-clarification"><strong>More detail needed</strong><span>Genie found the question ambiguous and needs clarification before querying telemetry.</span></div>}
    {response.answer && <div className="genie-answer-copy"><Markdown remarkPlugins={[remarkGfm]}>{response.answer}</Markdown></div>}
    {empty && <div className="genie-empty-inline"><strong>No answer was returned</strong><span>Try narrowing the time period, pump, or operating parameter.</span></div>}
    <ResultTable response={response} />
    {response.warnings.map((warning) => <div className="genie-partial" role="status" key={warning}>{warning} The answer and generated SQL are still available.</div>)}
    {response.queries.map((query) => <details className="genie-sql" key={query.attachment_id}>
      <summary>Generated SQL{query.title ? ` · ${query.title}` : ""}</summary>
      {query.description && <p>{query.description}</p>}
      <pre>{query.sql}</pre>
    </details>)}
    {response.suggested_questions.length > 0 && <div className="genie-suggestions"><span>Continue with</span>{response.suggested_questions.map((question) => <button type="button" key={question} onClick={() => onSuggestion(question)}>{question}</button>)}</div>}
    <footer className="genie-provenance">
      <div><strong>{response.source_label}</strong><span>{formatGeneratedAt(response.generated_at)}</span></div>
    </footer>
  </>;
}

export function GenieDrawer({ open, onClose }: { open: boolean; onClose: () => void }) {
  const [identity, setIdentity] = useState<GenieIdentity | null>(null);
  const [identityError, setIdentityError] = useState<string | null>(null);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [items, setItems] = useState<ConversationItem[]>([]);
  const [question, setQuestion] = useState("");
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const generation = useRef(0);
  const transcriptRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open || identity || identityError) return;
    void requestJson<GenieIdentity>("/api/genie/whoami")
      .then(setIdentity)
      .catch((caught) => setIdentityError(caught instanceof Error ? caught.message : "Unable to identify signed-in user"));
  }, [identity, identityError, open]);

  useEffect(() => {
    if (!open) return;
    const transcript = transcriptRef.current;
    if (transcript) transcript.scrollTop = transcript.scrollHeight;
  }, [items, open]);

  useEffect(() => {
    if (!open) return;
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, [onClose, open]);

  const updateResponse = (itemId: string, response: GenieMessageView) => {
    setItems((current) => current.map((item) => item.id === itemId ? { ...item, response } : item));
  };

  const pollMessage = async (itemId: string, submission: GenieSubmission, activeGeneration: number) => {
    try {
      while (generation.current === activeGeneration) {
        const response = await requestJson<GenieMessageView>(`/api/genie/messages/${encodeURIComponent(submission.message_id)}?conversation_id=${encodeURIComponent(submission.conversation_id)}`);
        if (generation.current !== activeGeneration) return;
        updateResponse(itemId, response);
        if (response.terminal) return;
        await new Promise((resolve) => window.setTimeout(resolve, 900));
      }
    } catch (caught) {
      if (generation.current === activeGeneration) setError(caught instanceof Error ? caught.message : "Unable to retrieve the Genie answer");
    } finally {
      if (generation.current === activeGeneration) setPending(false);
    }
  };

  const ask = async (content: string) => {
    const trimmed = content.trim();
    if (!trimmed || pending || !identity?.obo_ready) return;
    const itemId = crypto.randomUUID();
    const activeGeneration = generation.current;
    setError(null);
    setPending(true);
    setQuestion("");
    setItems((current) => [
      ...current,
      { id: crypto.randomUUID(), role: "user", content: trimmed },
      { id: itemId, role: "genie", content: "" }
    ]);
    try {
      const submission = await requestJson<GenieSubmission>("/api/genie/messages", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: trimmed, conversation_id: conversationId })
      });
      if (generation.current !== activeGeneration) return;
      setConversationId(submission.conversation_id);
      updateResponse(itemId, {
        ...submission,
        terminal: false,
        requires_clarification: false,
        queries: [],
        results: [],
        warnings: [],
        suggested_questions: [],
        source_label: "Unity Catalog · pump_telemetry_history"
      });
      await pollMessage(itemId, submission, activeGeneration);
    } catch (caught) {
      if (generation.current === activeGeneration) {
        setError(caught instanceof Error ? caught.message : "Unable to submit the Genie question");
        setPending(false);
      }
    }
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    void ask(question);
  };

  const newConversation = () => {
    generation.current += 1;
    setConversationId(null);
    setItems([]);
    setQuestion("");
    setPending(false);
    setError(null);
  };

  return <div className={`genie-shell ${open ? "genie-shell-open" : ""}`} aria-hidden={!open}>
    <button className="genie-backdrop" type="button" aria-label="Close Fleet telemetry analyst" tabIndex={open ? 0 : -1} onClick={onClose} />
    <aside className="genie-drawer" role="dialog" aria-modal="true" aria-label="Fleet telemetry analyst">
      <header className="genie-header">
        <div><p>Governed natural-language analytics</p><h2>Fleet telemetry analyst</h2></div>
        <div className="genie-header-actions"><button type="button" onClick={newConversation} disabled={!items.length}>New conversation</button><button className="genie-close" type="button" aria-label="Close Fleet telemetry analyst" onClick={onClose}>×</button></div>
      </header>
      <section className="genie-identity">
        <div className="genie-identity-mark">G</div>
        <div><strong>{identity?.email ?? identity?.display_name ?? "Identifying signed-in user…"}</strong><span>Queries run on your behalf with your Unity Catalog permissions</span></div>
        <span className={`genie-obo-badge ${identity?.obo_ready ? "ready" : ""}`}>{identity?.obo_ready ? "OBO active" : "OBO unavailable"}</span>
      </section>
      {identityError && <div className="genie-banner-error" role="alert">{identityError}</div>}
      <div className="genie-transcript" ref={transcriptRef}>
        {!items.length && <section className="genie-empty">
          <div className="genie-empty-symbol">⌁</div>
          <h3>Ask the continuous telemetry history</h3>
          <p>Explore vibration, temperature, pressure, flow, and motor-current measurements across the pump fleet. Demo scenarios and agent activity are intentionally outside this analyst's scope.</p>
          <div className="genie-starters">{STARTER_QUESTIONS.map((starter) => <button type="button" key={starter} onClick={() => void ask(starter)} disabled={!identity?.obo_ready || pending}>{starter}<span>→</span></button>)}</div>
        </section>}
        {items.map((item) => item.role === "user" ? <article className="genie-turn genie-turn-user" key={item.id}><span>You</span><p>{item.content}</p></article> : <article className="genie-turn genie-turn-answer" key={item.id}><div className="genie-turn-label"><span>Genie</span>{item.response && <small>{item.response.status_label}</small>}</div><GenieAnswer item={item} onSuggestion={(suggestion) => void ask(suggestion)} /></article>)}
        {error && <div className="genie-banner-error" role="alert"><strong>Request interrupted</strong><span>{error}</span><button type="button" onClick={() => setError(null)}>Dismiss</button></div>}
      </div>
      <form className="genie-composer" onSubmit={submit}>
        <label htmlFor="genie-question">Ask about fleet telemetry</label>
        <div><textarea id="genie-question" value={question} onChange={(event) => setQuestion(event.target.value)} placeholder="Ask about trends, comparisons, or threshold crossings…" rows={2} disabled={pending || !identity?.obo_ready} /><button type="submit" disabled={pending || !question.trim() || !identity?.obo_ready}>{pending ? "Working…" : "Ask Genie"}</button></div>
      </form>
    </aside>
  </div>;
}
