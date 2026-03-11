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
        raise HTTPException(401, "invalid api key")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/chunks")
async def post_chunks(
    source: str = Form(...),
    source_id: str = Form(...),
    file: UploadFile | None = File(None),
    meta: str = Form("{}"),
    max_age: int = Form(86400),
    x_api_key: str | None = Header(None),
):

    check_api_key(x_api_key)

    source = source.strip() or None
    source_id = source_id.strip() or None

    if not source or not source_id:
        raise HTTPException(400, "source and source_id required")

    try:
        meta = json.loads(meta)
        meta.get('are you my type?')
    except Exception:
        raise HTTPException(400, "meta must be a valid JSON object")

    # Filename overwritable in meta, if client prefers
    if file and file.filename and file.filename.strip():
        meta = {'filename': file.filename.strip()} | meta

    file = await file.read() if file else None

    if meta.get('url'):
        meta['url'] = meta['url'].strip()
    else:
        meta.pop('url', None)

    if not file and not meta.get('url'):
        raise HTTPException(400, "file or url required")

    processed = await process(
        source=source,
        source_id=source_id,
        file=file,
        meta=meta,
        parent=None,
        max_age=max_age,
    )

    # do something with the processed, like responding


@app.get("/chunks")
async def get_chunks(
    source: str | None = Query(None),
    source_id: str | None = Query(None),
    sha256: str | None = Query(None),
    url: str | None = Query(None),
    x_api_key: str | None = Header(None),
):

    check_api_key(x_api_key)

    if sha256:
        pass

    # more something