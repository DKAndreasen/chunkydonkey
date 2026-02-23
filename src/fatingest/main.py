# src/fatingest/main.py

from contextlib import asynccontextmanager
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, APIRouter
from .blobstorage import BlobStorage
from .parse import init_parse, parse


# --- LIFESPAN (GPU Loading) ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Fatingest: Initializing services and loading models...")
    await init_parse()
    yield
    print("Fatingest: Shutting down...")

app = FastAPI(title="Fatingest API", lifespan=lifespan)
router = APIRouter(prefix="/v1")


# --- ENDPOINTS ---


@app.get("/health")
async def health_endpoint():
    return {"status": "ok"}


@app.post("/parse")
async def parse_endpoint(
    file: UploadFile = File(...),
    use_cache: bool = Form(True)
):
    file_bytes = await file.read()
    result = await parse(file_bytes, file.filename or "", use_cache)
    return result[0]['markdown']