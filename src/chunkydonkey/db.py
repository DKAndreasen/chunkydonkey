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