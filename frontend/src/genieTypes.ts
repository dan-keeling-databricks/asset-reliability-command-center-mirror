export type GenieIdentity = {
  subject: string;
  email?: string | null;
  display_name: string;
  execution_mode: "on_behalf_of_user";
  obo_ready: boolean;
};

export type GenieSubmission = {
  conversation_id: string;
  message_id: string;
  status: string;
  status_label: string;
};

export type GenieQuery = {
  attachment_id: string;
  title?: string | null;
  description?: string | null;
  sql: string;
};

export type GenieResultTable = {
  attachment_id: string;
  columns: string[];
  rows: Array<Array<string | null>>;
  total_rows?: number | null;
  truncated: boolean;
};

export type GenieMessageView = {
  conversation_id: string;
  message_id: string;
  status: string;
  status_label: string;
  terminal: boolean;
  answer?: string | null;
  requires_clarification: boolean;
  queries: GenieQuery[];
  results: GenieResultTable[];
  warnings: string[];
  suggested_questions: string[];
  error?: string | null;
  source_label: string;
  generated_at?: string | null;
};
