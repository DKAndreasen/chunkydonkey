import asyncpg
import json
import os

_pool: asyncpg.Pool | None = None

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://chunkydonkey:chunkydonkey@postgres:5432/chunkydonkey")


async def init_pool():
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def pool() -> asyncpg.Pool:
    assert _pool is not None, "Database pool not initialized"
    return _pool


async def apply_schema():
    schema_path = os.path.join(os.path.dirname(__file__), "..", "..", "schema.sql")
    with open(schema_path) as f:
        sql = f.read()
    async with pool().acquire() as conn:
        await conn.execute(sql)


# -- Sources --


async def get_source(source: str, source_id: str) -> asyncpg.Record | None:
    async with pool().acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM sources WHERE source = $1 AND source_id = $2",
            source, source_id,
        )


async def bump_priority(source: str, source_id: str, priority: int):
    """Only increases priority, never decreases."""
    async with pool().acquire() as conn:
        await conn.execute(
            """UPDATE sources SET priority = GREATEST(priority, $3), updated_at = now()
               WHERE source = $1 AND source_id = $2""",
            source, source_id, priority,
        )


async def upsert_source(source: str, source_id: str, sha256: str, meta: dict | None = None, priority: int = 0):
    async with pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO sources (source, source_id, sha256, meta, priority)
               VALUES ($1, $2, $3, COALESCE($4::jsonb, '{}'::jsonb), $5)
               ON CONFLICT (source, source_id) DO UPDATE SET
                   sha256 = EXCLUDED.sha256,
                   meta = COALESCE(EXCLUDED.meta, sources.meta),
                   priority = GREATEST(sources.priority, EXCLUDED.priority),
                   updated_at = now()""",
            source, source_id, sha256, json.dumps(meta) if meta else None, priority,
        )


# -- Files --


async def upsert_file(sha256: str, content_type: str, meta: dict | None = None):
    async with pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO files (sha256, content_type, meta)
               VALUES ($1, $2, COALESCE($3::jsonb, '{}'::jsonb))
               ON CONFLICT (sha256) DO NOTHING""",
            sha256, content_type, json.dumps(meta) if meta else None,
        )


async def get_file(sha256: str) -> asyncpg.Record | None:
    async with pool().acquire() as conn:
        return await conn.fetchrow("SELECT * FROM files WHERE sha256 = $1", sha256)


async def set_processing(sha256: str):
    async with pool().acquire() as conn:
        await conn.execute(
            "UPDATE files SET processing_at = now() WHERE sha256 = $1", sha256
        )


async def set_processed(sha256: str, num_chunks: int):
    async with pool().acquire() as conn:
        await conn.execute(
            "UPDATE files SET processed_at = now(), num_chunks = $1 WHERE sha256 = $2",
            num_chunks, sha256,
        )


async def set_indexed(sha256: str):
    async with pool().acquire() as conn:
        await conn.execute(
            "UPDATE files SET indexed_at = now() WHERE sha256 = $1", sha256
        )


async def set_errored(sha256: str, error: str):
    async with pool().acquire() as conn:
        await conn.execute(
            """UPDATE files SET errored_at = now(),
               meta = jsonb_set(
                   COALESCE(meta, '{}'::jsonb),
                   '{errors}',
                   COALESCE(meta->'errors', '[]'::jsonb) || jsonb_build_array(
                       jsonb_build_object('at', now()::text, 'msg', $2::text)
                   )
               )
               WHERE sha256 = $1""",
            sha256, error,
        )


async def update_file_meta(sha256: str, meta: dict):
    async with pool().acquire() as conn:
        await conn.execute(
            "UPDATE files SET meta = meta || $2::jsonb WHERE sha256 = $1",
            sha256, json.dumps(meta, default=str),
        )


async def has_pending_chunks() -> bool:
    """Cheap check: are there any pending chunks at all?"""
    async with pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT EXISTS(SELECT 1 FROM chunks WHERE pending = TRUE) AS has"
        )
        return row["has"] if row else False


async def claim_next_pending_chunk() -> dict | None:
    """Claim the highest-priority pending chunk. Returns {chunk_sha256, file_sha256, idx} or None."""
    async with pool().acquire() as conn:
        row = await conn.fetchrow(
            """WITH target AS (
                   SELECT c.sha256
                   FROM chunks c
                   JOIN file_chunks fc ON fc.chunk_sha256 = c.sha256
                   JOIN sources s ON s.sha256 = fc.file_sha256
                   WHERE c.pending = TRUE
                   GROUP BY c.sha256, c.created_at
                   ORDER BY MAX(s.priority) DESC, c.created_at ASC
                   LIMIT 1
                   FOR UPDATE OF c SKIP LOCKED
               )
               UPDATE chunks SET pending = FALSE
               FROM target
               WHERE chunks.sha256 = target.sha256
               RETURNING chunks.sha256"""
        )
        if not row:
            return None
        chunk_sha = row["sha256"]
        fc = await conn.fetchrow(
            """SELECT chunk_sha256, file_sha256, idx
               FROM file_chunks WHERE chunk_sha256 = $1 LIMIT 1""",
            chunk_sha,
        )
        return dict(fc) if fc else None


async def count_pending_chunks(file_sha256: str) -> int:
    async with pool().acquire() as conn:
        row = await conn.fetchrow(
            """SELECT COUNT(*) as cnt FROM file_chunks fc
               JOIN chunks c ON c.sha256 = fc.chunk_sha256
               WHERE fc.file_sha256 = $1 AND c.pending = TRUE""",
            file_sha256,
        )
        return row["cnt"] if row else 0


# -- Chunks --


async def upsert_chunk(sha256: str, markdown: str, pending: bool = False):
    async with pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO chunks (sha256, markdown, pending)
               VALUES ($1, $2, $3)
               ON CONFLICT (sha256) DO NOTHING""",
            sha256, markdown, pending,
        )


async def update_chunk_markdown(sha256: str, markdown: str):
    async with pool().acquire() as conn:
        await conn.execute(
            "UPDATE chunks SET markdown = $2, pending = FALSE WHERE sha256 = $1",
            sha256, markdown,
        )


async def upsert_file_chunk(file_sha256: str, chunk_sha256: str, idx: int):
    async with pool().acquire() as conn:
        await conn.execute(
            """INSERT INTO file_chunks (file_sha256, chunk_sha256, idx)
               VALUES ($1, $2, $3)
               ON CONFLICT (file_sha256, idx) DO UPDATE SET chunk_sha256 = EXCLUDED.chunk_sha256""",
            file_sha256, chunk_sha256, idx,
        )


async def get_chunks(file_sha256: str) -> list[dict]:
    async with pool().acquire() as conn:
        rows = await conn.fetch(
            """SELECT c.sha256, c.markdown, c.pending, fc.idx
               FROM file_chunks fc
               JOIN chunks c ON c.sha256 = fc.chunk_sha256
               WHERE fc.file_sha256 = $1
               ORDER BY fc.idx""",
            file_sha256,
        )
        return [dict(r) for r in rows]


