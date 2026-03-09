import hashlib
import json
import logging
import os

from contextlib import asynccontextmanager
from fastapi import FastAPI, Form, Header, HTTPException, Query, UploadFile, File
from fastapi.responses import JSONResponse

from . import db
from .pipeline import process

logger = logging.getLogger(__name__)

API_KEY = os.getenv("API_KEY", "")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("chunkydonkey: initializing...")
    await db.init_pool()
    await db.apply_schema()
    yield
    logger.info("chunkydonkey: shutting down...")
    await db.close_pool()


app = FastAPI(title="chunkydonkey", lifespan=lifespan)


def check_api_key(key: str | None):
    if API_KEY and key != API_KEY:
        raise HTTPException(status_code=401, detail="invalid api key")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/chunks")
async def post_chunks(
    source: str = Form(...),
    source_id: str = Form(...),
    url: str | None = Form(None),
    file: UploadFile | None = File(None),
    meta: str = Form("{}"),
    x_api_key: str | None = Header(None),
    use_cache: bool = Form(True)
):

    check_api_key(x_api_key)

    source = source.strip() or None
    source_id = source_id.strip() or None
    if not source or not source_id:
        raise HTTPException(status_code=400, detail="source and source_id required")

    url = url if url.startswith("http://") or url.startswith("https://") else None
    file = await file.read() if file else None
    if not url and not file:
        raise HTTPException(status_code=400, detail="url or file required")

    try:
        meta = json.loads(meta)
    except Exception:
        raise HTTPException(status_code=400, detail="meta must be valid JSON")

    try:
        await process(
            source=source,
            source_id=source_id,
            source_meta=meta,
            url=url,
            file=file,
            use_cache=use_cache,
        )
    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))







@app.get("/chunks")
async def get_chunks(
    sha256: str | None = Query(None),
    source: str | None = Query(None),
    source_id: str | None = Query(None),
    priority: int | None = Query(None),
    x_api_key: str | None = Header(None),
):
    check_api_key(x_api_key)

    if sha256:
        row = await db.get_file(sha256)
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        # Bump priority if requested (needs source context)
        if priority is not None and source and source_id:
            await db.bump_priority(source, source_id, priority)
        return await build_file_response(sha256, row)

    if source and source_id:
        source_row = await db.get_source(source, source_id)
        if not source_row:
            raise HTTPException(status_code=404, detail="not found")
        if priority is not None:
            await db.bump_priority(source, source_id, priority)
        sha256 = source_row["sha256"]
        row = await db.get_file(sha256)
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        return await build_source_response(source, source_id, sha256, row, source_row)

    raise HTTPException(status_code=400, detail="provide sha256 or source+source_id")


def file_status(row) -> str:
    if row["errored_at"]:
        return "error"
    if row["indexed_at"]:
        return "indexed"
    if row["processed_at"]:
        return "processed"
    if row["processing_at"]:
        return "processing"
    return "queued"


def status_code_for(status: str) -> int:
    if status in ("processed", "indexed"):
        return 200
    if status == "processing":
        return 206
    return 202


async def build_file_response(sha256: str, row) -> JSONResponse:
    status = file_status(row)

    body = {
        "sha256": sha256,
        "status": status,
        "content_type": row["content_type"],
        "num_chunks": row["num_chunks"],
        "meta": row["meta"] if row["meta"] else {},
        "chunks": await db.get_chunks(sha256),
    }

    return JSONResponse(content=body, status_code=status_code_for(status))


async def build_source_response(source: str, source_id: str, sha256: str, file_row, source_row=None) -> JSONResponse:
    status = file_status(file_row)

    if not source_row:
        source_row = await db.get_source(source, source_id)

    body = {
        "source": source,
        "source_id": source_id,
        "source_meta": dict(source_row["meta"]) if source_row and source_row["meta"] else {},
        "sha256": sha256,
        "status": status,
        "content_type": file_row["content_type"],
        "num_chunks": file_row["num_chunks"],
        "file_meta": file_row["meta"] if file_row["meta"] else {},
        "chunks": await db.get_chunks(sha256),
    }

    return JSONResponse(content=body, status_code=status_code_for(status))
