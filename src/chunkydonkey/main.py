import json
import logging
import os

from contextlib import asynccontextmanager
from fastapi import FastAPI, Form, Header, HTTPException, Query, UploadFile, File
from fastapi.responses import JSONResponse

from . import db
from .url_to_file import url_to_file
from .archive_to_files import archive_to_files
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
    url: str = Form(""),
    file: UploadFile | None = File(None),
    meta: str = Form("{}"),
    use_cache: bool = Form(True),
    x_api_key: str | None = Header(None),
):

    check_api_key(x_api_key)

    system_meta = {}

    source = source.strip() or None
    source_id = source_id.strip() or None
    if not source or not source_id:
        raise HTTPException(400, "source and source_id required")

    url = url.strip()
    filename = file.filename if file else ""
    file = await file.read() if file else None

    # URL route
    if url:
        system_meta['url'] = url
        if not file:
            file, resolved_url = await url_to_file(url)
            system_meta['resolved_url'] = resolved_url

    if not file:
        raise HTTPException(400, "url or file required")

    # Archive route
    files = archive_to_files(file)
    if files:
        if filename:
            system_meta['archive'] = filename
    else:
        files = [(filename, file)]

    try:
        source_meta = json.loads(meta)
    except Exception:
        raise HTTPException(400, "meta must be valid JSON")

    response = [
        await process(
            source=source,
            source_id=source_id,
            source_meta=source_meta,
            system_meta=system_meta | ({'filename': f_name} if f_name else {}),
            file=f_bytes,
            use_cache=use_cache,
        ) 
        for f_name, f_bytes in files
    ]


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
        pass