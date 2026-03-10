-- chunkydonkey pipeline schema
-- Requires: pgvector, vectorscale (diskann), pg_trgm

CREATE EXTENSION IF NOT EXISTS vectorscale CASCADE;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================================
-- Files: content-addressable store
--
-- sha256 keys the raw file in object storage (SeaweedFS).
-- markdown is the converted source of truth (NULL for archives).
-- meta holds extracted metadata (content_type, dimensions, etc).
-- State: created_at exists, indexed_at means chunks are derived.
-- On restart, re-index anything with created_at but no indexed_at.
-- ============================================================
CREATE TABLE files (
    sha256        TEXT PRIMARY KEY,
    meta          JSONB DEFAULT '{}',
    markdown      TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    indexed_at    TIMESTAMPTZ,
    touched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_files_unfinished ON files(created_at) WHERE indexed_at IS NULL;

-- ============================================================
-- URL → File resolution cache
--
-- Shared across sources. Same URL = same resolution.
-- fetched_at enables max_age freshness checks.
-- ============================================================
CREATE TABLE url_files (
    url           TEXT PRIMARY KEY,
    resolved_url  TEXT,
    file_sha256   TEXT NOT NULL REFERENCES files(sha256) ON DELETE CASCADE,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_url_files_sha256 ON url_files(file_sha256);

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
-- url is set for URL-based sources (refreshable), NULL for direct file uploads.
-- On re-fetch with new content, file_sha256 is updated; orphan cleanup handles the old file.
-- ============================================================
CREATE TABLE sources (
    source       TEXT NOT NULL,
    source_id    TEXT NOT NULL,
    file_sha256  TEXT NOT NULL REFERENCES files(sha256) ON DELETE CASCADE,
    url          TEXT,
    meta         JSONB DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    touched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_id)
);

CREATE INDEX idx_sources_sha256 ON sources(file_sha256);

-- ============================================================
-- File → File mapping (containment)
--
-- Archives contain files, documents contain images, videos contain stills.
-- ON DELETE CASCADE on both sides: parent or child deleted →
-- relationship removed. Orphan children cleaned up by next sweep.
-- ============================================================
CREATE TABLE file_files (
    parent_sha256  TEXT NOT NULL REFERENCES files(sha256) ON DELETE CASCADE,
    child_sha256   TEXT NOT NULL REFERENCES files(sha256) ON DELETE CASCADE,
    meta           JSONB DEFAULT '{}',
    PRIMARY KEY (parent_sha256, child_sha256)
);

CREATE INDEX idx_file_files_child ON file_files(child_sha256);

-- ============================================================
-- Chunks: derived search index
--
-- Generated from files.markdown with VLM output injected from
-- child image files via <!--img:sha256-->...<!--/img:sha256-->.
-- Rebuildable from source of truth (files.markdown + file_files).
-- sha256 = hash(assembled markdown including VLM output).
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
CREATE INDEX idx_chunks_unembedded ON chunks(created_at) WHERE embedding IS NULL;

-- ============================================================
-- File → Chunk mapping (ordered)
--
-- Derived index linking files to their assembled chunks.
-- Lineage: "which documents contain this chunk?" via chunk_sha256.
-- Context expansion: adjacent chunks by idx for surrounding context.
-- ============================================================
CREATE TABLE file_chunks (
    file_sha256  TEXT NOT NULL REFERENCES files(sha256) ON DELETE CASCADE,
    chunk_sha256 TEXT NOT NULL REFERENCES chunks(sha256),
    idx          INTEGER NOT NULL,
    PRIMARY KEY (file_sha256, idx)
);

CREATE INDEX idx_file_chunks_chunk ON file_chunks(chunk_sha256);
