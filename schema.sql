-- chunkydonkey pipeline schema
-- Requires: pgvector, vectorscale (diskann), pg_trgm

CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- Files: content-addressable store metadata + pipeline state
--
-- The file itself is NOT stored — only metadata and state.
-- State is simple: created_at exists, indexed_at means done.
-- On restart, resume anything with created_at but no indexed_at.
-- ============================================================
CREATE TABLE files (
    sha256        TEXT PRIMARY KEY,
    meta          JSONB DEFAULT '{}',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    indexed_at    TIMESTAMPTZ,
    touched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_files_unfinished ON files(created_at) WHERE indexed_at IS NULL;

-- ============================================================
-- Sources: where content comes from
--
-- Composite PK (source, source_id) enables natural grouping:
--   source = "intranet",                source_id = "https://intranet.example.com/page"
--   source = "sharepoint/ai-vidensbank", source_id = "sti/til/fil.pdf"
--   source = "user/123",                source_id = "session/456/rapport.pdf"
--
-- Access control is handled at query time by filtering on source.
-- Multiple sources can point to the same file (dedup via sha256).
-- On re-ingest with new content, sha256 is updated; orphan cleanup handles the old file.
-- ============================================================
CREATE TABLE sources (
    source       TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    source_meta  JSONB DEFAULT '{}',
    system_meta  JSONB DEFAULT '{}',
    file_sha256  TEXT NOT NULL REFERENCES files(sha256),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    touched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_id)
);

CREATE INDEX idx_sources_source ON sources(source);
CREATE INDEX idx_sources_sha256 ON sources(file_sha256);

-- ============================================================
-- Chunks: content units
--
-- sha256 = hash(markdown content)
-- Deterministic — same content always gives same hash.
-- Images are referenced as sha256 hashes in markdown, not inlined.
-- ============================================================
CREATE TABLE chunks (
    sha256       TEXT PRIMARY KEY,
    markdown     TEXT NOT NULL,
    embedding    vector(1024),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    retrieved_at TIMESTAMPTZ
);

CREATE INDEX idx_chunks_markdown_trgm ON chunks USING GIN(markdown gin_trgm_ops);
CREATE INDEX idx_chunks_embedding ON chunks USING diskann(embedding vector_cosine_ops);

-- ============================================================
-- File → Chunk mapping (ordered)
--
-- Enables lineage tracking: "which documents contain this page?"
-- is a simple GROUP BY on chunk_sha256.
-- ============================================================
CREATE TABLE file_chunks (
    file_sha256  TEXT NOT NULL REFERENCES files(sha256) ON DELETE CASCADE,
    chunk_sha256 TEXT NOT NULL REFERENCES chunks(sha256),
    idx          INTEGER NOT NULL,
    PRIMARY KEY (file_sha256, idx)
);

CREATE INDEX idx_file_chunks_chunk ON file_chunks(chunk_sha256);
