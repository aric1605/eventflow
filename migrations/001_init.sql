-- 001_init.sql: Schema for EventFlow

CREATE TABLE IF NOT EXISTS schema_migrations (
  id serial PRIMARY KEY,
  name text NOT NULL UNIQUE,
  applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS events (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  type text NOT NULL,
  payload jsonb NOT NULL,
  payload_hash text NOT NULL,
  idempotency_key text UNIQUE NULL,
  status text NOT NULL CHECK (status IN ('received', 'queued', 'processing', 'succeeded', 'retrying', 'dead')),
  attempts int NOT NULL DEFAULT 0,
  max_attempts int NOT NULL,
  next_attempt_at timestamptz NULL,
  last_error text NULL,
  result jsonb NULL,
  stream_id text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  queued_at timestamptz NULL,
  started_at timestamptz NULL,
  finished_at timestamptz NULL,
  processing_ms int NULL
);

CREATE TABLE IF NOT EXISTS event_attempts (
  id bigserial PRIMARY KEY,
  event_id uuid NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  attempt int NOT NULL,
  started_at timestamptz NOT NULL,
  finished_at timestamptz NOT NULL,
  outcome text NOT NULL CHECK (outcome IN ('success', 'error', 'timeout', 'permanent', 'crashed')),
  error text NULL,
  duration_ms int NOT NULL
);

-- Core indexes for queries, sweeper, and status transitions
CREATE INDEX IF NOT EXISTS idx_events_status_created_at ON events(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_status_next_attempt_at ON events(status, next_attempt_at) WHERE status = 'retrying';
CREATE INDEX IF NOT EXISTS idx_events_status_started_at ON events(status, started_at) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_event_attempts_event_id ON event_attempts(event_id);
