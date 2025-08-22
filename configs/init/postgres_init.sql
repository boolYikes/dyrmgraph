CREATE EXTENSION IF NOT EXISTS pgvector;

CREATE TABLE IF NOT EXISTS events (
  id           BIGSERIAL PRIMARY KEY,
  gdelt_id     BIGINT,
  event_text   TEXT,
  embedding    VECTOR(1024),
  sql_date     DATE,
  actor1       TEXT,
  actor2       TEXT,
  source_url   TEXT
);

-- IVFFlat index for cosine similarity
CREATE INDEX IF NOT EXISTS events_embedding_idx
ON events
USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
