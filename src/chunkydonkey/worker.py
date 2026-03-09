"""
OCR Worker — runs as a separate process.

Maintains N concurrent VLM requests via semaphore.
Claims one chunk at a time, sorted by source priority DESC, created_at ASC.
"""

import asyncio
import logging
import os

import pymupdf

from . import db
from .pipeline import compute_page_dpi
from .vlm import ocr_pdf_page

logger = logging.getLogger(__name__)

CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "8"))


# PDF document cache shared across tasks
pdf_cache: dict[str, pymupdf.Document] = {}
pdf_lock = asyncio.Lock()


def get_pdf(file_sha: str) -> pymupdf.Document | None:
    doc = pdf_cache.get(file_sha)
    if doc:
        return doc
    pdf_path = f"/tmp/chunkydonkey/{file_sha}.pdf"
    if not os.path.exists(pdf_path):
        return None
    doc = pymupdf.open(pdf_path)
    pdf_cache[file_sha] = doc
    return doc


def close_pdf(file_sha: str):
    doc = pdf_cache.pop(file_sha, None)
    if doc:
        doc.close()
    pdf_path = f"/tmp/chunkydonkey/{file_sha}.pdf"
    try:
        os.remove(pdf_path)
    except OSError:
        pass


async def process_chunk(item: dict, sem: asyncio.Semaphore):
    chunk_sha = item["chunk_sha256"]
    file_sha = item["file_sha256"]
    page_idx = item["idx"]

    try:
        # Check if this is a standalone image chunk (saved by pipeline)
        img_path = f"/tmp/chunkydonkey/{chunk_sha}.img"
        is_image = os.path.exists(img_path)

        if is_image:
            with open(img_path, "rb") as f:
                img_bytes = f.read()
            text_layer = ""
            os.remove(img_path)
            row = await db.get_file(file_sha)
            mime = row["content_type"] if row else "image/png"
        else:
            # PDF page chunk
            doc = get_pdf(file_sha)
            if doc is None:
                logger.error(f"PDF not found for {file_sha[:12]}")
                return

            page = doc[page_idx]
            dpi = compute_page_dpi(page)
            pix = page.get_pixmap(dpi=dpi)
            img_bytes = pix.tobytes("jpeg", jpg_quality=85)
            text_layer = page.get_text("text")
            mime = "image/jpeg"

        markdown = await ocr_pdf_page(img_bytes, text_layer, mime=mime)

        if markdown.strip():
            await db.update_chunk_markdown(chunk_sha, markdown)
            label = "image" if is_image else f"page={page_idx}"
            logger.info(f"OCR done: file={file_sha[:12]} {label}")
        else:
            logger.warning(f"VLM empty: file={file_sha[:12]} page={page_idx}")

    except Exception as e:
        logger.exception(f"OCR failed: chunk={chunk_sha[:12]} error={e}")

    finally:
        # Check if this file is now complete
        remaining = await db.count_pending_chunks(file_sha)
        if remaining == 0:
            row = await db.get_file(file_sha)
            if row and not row["processed_at"]:
                await db.set_processed(file_sha, row["num_chunks"])
                close_pdf(file_sha)
                logger.info(f"File complete: {file_sha[:12]}")

        sem.release()


async def run_worker():
    logger.info(f"OCR worker starting (concurrency={CONCURRENCY})...")
    await db.init_pool()

    sem = asyncio.Semaphore(CONCURRENCY)

    while True:
        try:
            await sem.acquire()

            # Cheap check first
            if not await db.has_pending_chunks():
                sem.release()
                await asyncio.sleep(1)
                continue

            # Claim highest priority chunk
            item = await db.claim_next_pending_chunk()
            if not item:
                sem.release()
                await asyncio.sleep(1)
                continue

            asyncio.create_task(process_chunk(item, sem))

        except Exception as e:
            logger.exception(f"Worker feeder error: {e}")
            sem.release()
            await asyncio.sleep(1)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
