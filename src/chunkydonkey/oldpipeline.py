"""
Pipeline routing:

1.  is_url?      → playwright (browserless) → HTML + DOM images, or downloaded file
2a. is_html?     → extract trafilatura meta + base64 images from <img> tags
2b.              → trafilatura → markdown (with url for resolving relative links)
2c. is_markdown? → resolve external image refs → linkify bare URLs → extract links
    hash here:   → sha256(markdown + sorted image bytes + sorted meta json)
3.               → gotenberg /chromium/convert/markdown + images → PDF
4.  is_office?   → extract office meta (ZIP stdlib) → gotenberg /libreoffice/convert → PDF
5.  is_pdf?      → pymupdf4llm → page chunks (linkify + extract links per page)
6.  is_tabular?  → polars → adaptive chunking → markdown with headers
7.  is_image?    → normalize to JPEG → single pending chunk → VLM worker OCR

Meta (files.meta) accumulates through the pipeline and is included in the hash.
Links are extracted from all markdown outputs (HTML, PDF, office) into meta.
"""

import asyncio
import base64
import hashlib
import io
import ipaddress
import json
import logging
import math
import os
import re
import socket
import xml.etree.ElementTree as ET
import zipfile
from urllib.parse import urlparse

import clevercsv
import filetype
import httpx
import polars as pl
import pymupdf
import pymupdf.layout
import pymupdf4llm
import trafilatura
from playwright.async_api import async_playwright

from . import db

logger = logging.getLogger(__name__)

GOTENBERG_URL = os.getenv("GOTENBERG_URL", "http://gotenberg:3000")
BROWSERLESS_URL = os.getenv("BROWSERLESS_URL", "ws://browserless:3000")
MAX_PIXELS = 1024 * 1024
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(100 * 1024 * 1024)))

DATA_URI_RE = re.compile(
    r'''(src\s*=\s*)(["'])data:image/([^;]+);base64,([^"']+)\2''',
    re.IGNORECASE,
)
MD_IMAGE_RE = re.compile(r'!\[([^\]]*)\]\(([^)]+)\)')
MD_LINK_RE = re.compile(r'\[([^\]]*)\]\(([^)]+)\)')
BARE_URL_RE = re.compile(r'(?<!\]\()(?<!\()(?<!")(https?://[^\s\)\]>"]+)', re.IGNORECASE)


def compute_page_dpi(page: pymupdf.Page) -> int:
    """Compute DPI that keeps total pixels strictly under MAX_PIXELS."""
    rect = page.rect
    width_in = rect.width / 72
    height_in = rect.height / 72
    area_in2 = width_in * height_in
    if area_in2 <= 0:
        return 72
    return max(math.floor((MAX_PIXELS / area_in2) ** 0.5) - 1, 72)


async def is_safe_url(url: str) -> bool:
    """Prevent SSRF by rejecting URLs that resolve to private/loopback/link-local IPs."""
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return False
        scheme = (parsed.scheme or "").lower()
        if scheme not in ("http", "https"):
            return False
        try:
            ip = ipaddress.ip_address(hostname)
            return not (ip.is_private or ip.is_loopback or ip.is_link_local)
        except ValueError:
            pass
        loop = asyncio.get_running_loop()
        try:
            addr_info = await loop.getaddrinfo(hostname, None)
        except socket.gaierror:
            return False
        for _, _, _, _, sockaddr in addr_info:
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local:
                return False
        return True
    except Exception:
        return False


def compute_content_hash(md_bytes: bytes, images: dict[str, bytes], file_meta: dict) -> str:
    """Hash markdown + all image bytes + meta for content-addressable dedup."""
    h = hashlib.sha256()
    h.update(md_bytes)
    for filename in sorted(images.keys()):
        h.update(images[filename])
    h.update(json.dumps(file_meta, sort_keys=True, default=str).encode("utf-8"))
    return h.hexdigest()


def normalize_text(file_bytes: bytes) -> bytes:
    """Try multiple encodings, normalize EOL, return clean UTF-8 bytes."""
    for encoding in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
        try:
            text = file_bytes.decode(encoding)
            text = text.replace("\r\n", "\n").replace("\r", "\n")
            return text.encode("utf-8")
        except (UnicodeDecodeError, LookupError):
            continue
    return file_bytes


def linkify_urls(markdown: str) -> str:
    """Convert bare URLs to markdown links: https://x → [https://x](https://x)."""
    return BARE_URL_RE.sub(r'[\1](\1)', markdown)


def extract_links(markdown: str) -> list[str]:
    """Extract all [text](url) link URLs from markdown (excluding images)."""
    links = []
    for match in MD_LINK_RE.finditer(markdown):
        url = match.group(2).strip()
        # Skip image refs (preceded by !)
        start = match.start()
        if start > 0 and markdown[start - 1] == "!":
            continue
        if url and not url.startswith(("#", "data:")):
            links.append(url)
    return links


def extract_trafilatura_meta(html_text: str, url: str | None = None) -> dict:
    """Extract stable metadata from HTML via trafilatura."""
    meta_obj = trafilatura.extract_metadata(html_text, default_url=url)
    if not meta_obj:
        return {}
    meta = {}
    for field in ("title", "author", "date", "description", "sitename",
                   "license", "pagetype"):
        val = getattr(meta_obj, field, None)
        if val and str(val).strip():
            meta[field] = str(val).strip()
    for field in ("categories", "tags"):
        val = getattr(meta_obj, field, None)
        if val:
            meta[field] = list(val)
    # filedate is non-deterministic (today's date) — excluded
    return meta


def extract_office_meta(file_bytes: bytes) -> dict:
    """Extract metadata from modern office formats (.docx, .xlsx, .pptx) via ZIP stdlib."""
    meta = {}
    try:
        with zipfile.ZipFile(io.BytesIO(file_bytes)) as zf:
            # Dublin Core metadata in docProps/core.xml
            if "docProps/core.xml" in zf.namelist():
                tree = ET.parse(zf.open("docProps/core.xml"))
                root = tree.getroot()
                ns = {
                    "dc": "http://purl.org/dc/elements/1.1/",
                    "dcterms": "http://purl.org/dc/terms/",
                    "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
                }
                for tag, key in [
                    ("dc:title", "title"),
                    ("dc:creator", "author"),
                    ("dc:subject", "subject"),
                    ("dc:description", "description"),
                    ("cp:keywords", "keywords"),
                    ("cp:category", "category"),
                    ("cp:lastModifiedBy", "last_modified_by"),
                    ("dcterms:created", "created"),
                    ("dcterms:modified", "modified"),
                ]:
                    elem = root.find(tag, ns)
                    if elem is not None and elem.text and elem.text.strip():
                        meta[key] = elem.text.strip()
            # Application info in docProps/app.xml
            if "docProps/app.xml" in zf.namelist():
                tree = ET.parse(zf.open("docProps/app.xml"))
                root = tree.getroot()
                ns_app = {"ep": "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"}
                for tag, key in [
                    ("ep:Application", "application"),
                    ("ep:Company", "company"),
                ]:
                    elem = root.find(tag, ns_app)
                    if elem is not None and elem.text and elem.text.strip():
                        meta[key] = elem.text.strip()
    except (zipfile.BadZipFile, ET.ParseError, KeyError):
        pass
    return meta


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


async def process(
    file_bytes: bytes | None,
    source: str,
    source_id: str,
    meta: dict,
    priority: int = 0,
) -> str:
    images: dict[str, bytes] = {}  # filename → bytes, carried through pipeline
    file_meta: dict = {}  # accumulated metadata about the file

    # Step 1: URL fetch (source_id is the URL)
    fetch_url_str: str | None = None
    if file_bytes is None:
        fetch_url_str = source_id
        file_bytes, images = await fetch_url(fetch_url_str)

    ft = filetype.guess(file_bytes)
    if ft:
        content_type = ft.mime
    else:
        try:
            file_bytes[:1024].decode("utf-8")
            content_type = "text/plain"
        except UnicodeDecodeError:
            content_type = "application/octet-stream"

    # Normalize encoding for text-based inputs (HTML, markdown, CSV, JSON, etc.)
    if ft is None and content_type in ("text/plain", "text/html"):
        file_bytes = normalize_text(file_bytes)

    # Step 2a: HTML → extract base64 images → trafilatura meta → markdown
    has_markdown = False
    if is_html(file_bytes, ft):
        html_text = file_bytes.decode("utf-8", errors="replace")
        file_meta.update(extract_trafilatura_meta(html_text, url=fetch_url_str))
        file_bytes, images = extract_base64_images(file_bytes, images)
        file_bytes, images = convert_html_to_markdown(file_bytes, images, url=fetch_url_str)
        has_markdown = True

    # Office metadata — extract before conversion (ZIP-based, no extra deps)
    if is_office(ft):
        file_meta.update(extract_office_meta(file_bytes))

    # Step 2b: Markdown → resolve images → linkify bare URLs → extract links
    if has_markdown:
        md_text = file_bytes.decode("utf-8", errors="replace")
        md_text, images = await resolve_images(md_text, images)
        md_text = linkify_urls(md_text)
        file_meta["links"] = extract_links(md_text)
        file_bytes = md_text.encode("utf-8")
        sha256 = compute_content_hash(file_bytes, images, file_meta)
    else:
        sha256 = hashlib.sha256(file_bytes).hexdigest()

    # Dedup — if file already exists in any state, just link source and return
    existing = await db.get_file(sha256)
    if existing:
        await db.upsert_source(source, source_id, sha256, meta, priority)
        return sha256

    # New file — register and process (content_type is always the original)
    await db.upsert_file(sha256, content_type, file_meta)
    await db.upsert_source(source, source_id, sha256, meta, priority)
    await db.set_processing(sha256)

    try:
        if has_markdown:
            file_bytes = await convert_markdown_to_pdf(file_bytes, images)

        if is_office(ft):
            file_bytes = await convert_office_to_pdf(file_bytes, ft)

        if is_pdf(file_bytes):
            await extract_pdf_chunks(sha256, file_bytes, file_meta)
            return sha256

        if is_tabular(file_bytes, ft):
            await extract_tabular_chunks(sha256, file_bytes, file_meta)
            return sha256

        if is_image(ft):
            await extract_image_chunk(sha256, file_bytes, file_meta)
            return sha256

        await db.set_errored(sha256, "unsupported format")

    except Exception as e:
        logger.exception(f"Pipeline error for {sha256}: {e}")
        await db.set_errored(sha256, str(e))

    return sha256


# ---------------------------------------------------------------------------
# Type detection
# ---------------------------------------------------------------------------


def is_html(file_bytes: bytes, ft) -> bool:
    if ft and ft.mime in ("text/html", "application/xhtml+xml"):
        return True
    if ft is None:
        try:
            head = file_bytes[:512].decode("utf-8", errors="ignore").strip().lower()
            return head.startswith("<!doctype html") or head.startswith("<html") or "<head" in head
        except Exception:
            pass
    return False


def is_pdf(file_bytes: bytes) -> bool:
    return file_bytes[:5] == b"%PDF-"


def is_office(ft) -> bool:
    return ft is not None and ft.extension in ("doc", "docx", "ppt", "pptx", "odp", "xls", "xlsx", "ods")


def is_image(ft) -> bool:
    return ft is not None and ft.mime.startswith("image/")


def is_tabular(file_bytes: bytes, ft) -> bool:
    if ft and ft.extension in ("xls", "xlsx", "ods"):
        return False  # office → PDF path
    if file_bytes[:4] == b"PAR1":
        return True
    try:
        head = file_bytes[:256].decode("utf-8", errors="ignore").strip()
        if head.startswith("[") or head.startswith("{"):
            return True
        lines = head.split("\n")
        if len(lines) >= 2:
            dialect = clevercsv.Sniffer().sniff(head, verbose=False)
            if dialect and dialect.delimiter:
                return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Step 1: URL fetch via Playwright + browserless
# ---------------------------------------------------------------------------


async def fetch_url(url: str) -> tuple[bytes, dict[str, bytes]]:
    """Fetch URL via browserless. Returns (file_bytes, images_dict).

    If the page is HTML, captures DOM + images loaded by the browser.
    If the page triggers a download (PDF, etc.), returns the downloaded bytes.
    """
    if not await is_safe_url(url):
        raise ValueError(f"URL blocked by SSRF protection: {url}")

    captured_images: dict[str, bytes] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.connect(BROWSERLESS_URL)
        try:
            page = await browser.new_page()

            # Block non-essential resources (keep images, block fonts/media)
            async def route_handler(route):
                if route.request.resource_type in ("font", "media"):
                    await route.abort()
                else:
                    await route.continue_()

            await page.route("**/*", route_handler)

            # Capture image responses for later use in markdown → PDF
            async def on_response(response):
                ct = response.headers.get("content-type", "")
                if ct.startswith("image/") and response.ok:
                    try:
                        body = await response.body()
                        path_part = urlparse(response.url).path
                        filename = path_part.split("/")[-1] or f"image_{len(captured_images)}.png"
                        captured_images[filename] = body
                    except Exception:
                        pass

            page.on("response", on_response)

            # Download detection (litecrawl pattern)
            downloads: list = []
            download_event = asyncio.Event()

            def on_download(d):
                downloads.append(d)
                download_event.set()

            page.on("download", on_download)

            try:
                response = await page.goto(url, wait_until="load", timeout=30000)
            except Exception as exc:
                # Navigation can fail when browser triggers a download instead.
                # Wait briefly for the download event before giving up.
                try:
                    await asyncio.wait_for(download_event.wait(), timeout=0.1)
                except TimeoutError:
                    raise exc

                if not downloads:
                    raise exc

                # It was a download — read and return file bytes
                download_path = await downloads[-1].path()
                if not download_path:
                    raise RuntimeError(f"Download save failed for {url}")
                with open(download_path, "rb") as f:
                    file_bytes = f.read()
                if len(file_bytes) > MAX_FILE_SIZE:
                    raise ValueError(f"Download too large: {len(file_bytes)} bytes")
                return file_bytes, {}

            # HTML path
            if response is None or not response.ok:
                status = response.status if response else "no response"
                raise RuntimeError(f"Fetch failed for {url}: HTTP {status}")

            # Pre-check content-length header if available
            cl = response.headers.get("content-length", "")
            if cl.isdigit() and int(cl) > MAX_FILE_SIZE:
                raise ValueError(f"Response too large: {cl} bytes")

            # Wait for DOM images to finish loading (bounded)
            try:
                await asyncio.wait_for(
                    page.evaluate("""() => Promise.all(
                        Array.from(document.querySelectorAll('img'))
                            .filter(img => !img.complete)
                            .map(img => new Promise(r => {
                                img.addEventListener('load', r, {once: true});
                                img.addEventListener('error', r, {once: true});
                            }))
                    )"""),
                    timeout=10,
                )
            except Exception:
                pass

            html_content = await page.content()
            html_bytes = html_content.encode("utf-8")

            if len(html_bytes) > MAX_FILE_SIZE:
                raise ValueError(f"HTML content too large: {len(html_bytes)} bytes")

            return html_bytes, captured_images

        finally:
            try:
                page.off("download", on_download)
                page.off("response", on_response)
            except Exception:
                pass
            await browser.close()


# ---------------------------------------------------------------------------
# Step 2a: Extract base64 images from HTML (before trafilatura drops them)
# ---------------------------------------------------------------------------


def extract_base64_images(html_bytes: bytes, images: dict[str, bytes]) -> tuple[bytes, dict[str, bytes]]:
    """Find <img src="data:image/...;base64,...">, decode to bytes, replace with local filename."""
    html = html_bytes.decode("utf-8", errors="replace")
    counter = [len(images)]

    def replacer(match):
        prefix = match.group(1)   # 'src=' or 'src = '
        quote = match.group(2)    # '"' or "'"
        img_type = match.group(3) # png, jpeg, gif, webp, etc.
        b64_data = match.group(4)
        try:
            img_bytes = base64.b64decode(b64_data)
        except Exception:
            return match.group(0)
        ext = img_type.split("+")[0]
        if ext == "jpeg":
            ext = "jpg"
        filename = f"b64_{counter[0]}.{ext}"
        counter[0] += 1
        images[filename] = img_bytes
        return f"{prefix}{quote}{filename}{quote}"

    html = DATA_URI_RE.sub(replacer, html)
    return html.encode("utf-8"), images


# ---------------------------------------------------------------------------
# Step 2b: HTML → Markdown via trafilatura
# ---------------------------------------------------------------------------


def convert_html_to_markdown(
    html_bytes: bytes, images: dict[str, bytes], url: str | None = None,
) -> tuple[bytes, dict[str, bytes]]:
    html_text = html_bytes.decode("utf-8", errors="replace")

    markdown = trafilatura.extract(
        html_text,
        url=url,
        output_format="markdown",
        include_images=True,
        include_links=True,
    )

    if not markdown:
        markdown = ""

    return markdown.encode("utf-8"), images


# ---------------------------------------------------------------------------
# Step 2c: Resolve external image refs in markdown
# ---------------------------------------------------------------------------


async def resolve_images(markdown: str, images: dict[str, bytes]) -> tuple[str, dict[str, bytes]]:
    """Find ![alt](url) refs, fetch missing images, rewrite to local filenames."""
    urls_to_fetch: dict[str, str] = {}
    counter = [len(images)]

    def replacer(match):
        alt = match.group(1)
        url = match.group(2).strip()
        if url.startswith(("data:", "#")):
            return match.group(0)
        # Check if already captured (e.g., from playwright) by filename match
        filename = urlparse(url).path.split("/")[-1]
        if filename and filename in images:
            return f"![{alt}]({filename})"
        # Already scheduled for fetch?
        if url in urls_to_fetch:
            return f"![{alt}]({urls_to_fetch[url]})"
        # New image — assign filename, schedule fetch
        ext = os.path.splitext(filename)[1] if filename else ""
        if not ext:
            ext = ".png"
        new_filename = f"img_{counter[0]}{ext}"
        counter[0] += 1
        urls_to_fetch[url] = new_filename
        return f"![{alt}]({new_filename})"

    markdown = MD_IMAGE_RE.sub(replacer, markdown)

    if urls_to_fetch:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0)) as client:

            async def fetch_one(url: str, filename: str):
                if not await is_safe_url(url):
                    return
                try:
                    resp = await client.get(url)
                    resp.raise_for_status()
                    if len(resp.content) <= MAX_FILE_SIZE:
                        images[filename] = resp.content
                except Exception:
                    pass

            await asyncio.gather(*[
                fetch_one(url, fn) for url, fn in urls_to_fetch.items()
            ])

    return markdown, images


# ---------------------------------------------------------------------------
# Step 3: Markdown → PDF via Gotenberg
# ---------------------------------------------------------------------------


async def convert_markdown_to_pdf(md_bytes: bytes, images: dict[str, bytes]) -> bytes:
    endpoint = f"{GOTENBERG_URL}/forms/chromium/convert/markdown"

    wrapper = '<!DOCTYPE html>\n<html><head><meta charset="utf-8"></head>\n<body>{{ toHTML .DirPath "content.md" }}</body>\n</html>'

    files = {
        "index.html": ("index.html", wrapper.encode("utf-8"), "text/html"),
        "content.md": ("content.md", md_bytes, "text/markdown"),
    }

    for filename, img_bytes in images.items():
        ft = filetype.guess(img_bytes)
        mime = ft.mime if ft else "image/png"
        files[filename] = (filename, img_bytes, mime)

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
        response = await client.post(endpoint, files=files)
        response.raise_for_status()
        return response.content


# ---------------------------------------------------------------------------
# Step 4: Office → PDF via Gotenberg
# ---------------------------------------------------------------------------


async def convert_office_to_pdf(file_bytes: bytes, ft) -> bytes:
    endpoint = f"{GOTENBERG_URL}/forms/libreoffice/convert"
    form_name = f"document.{ft.extension}"

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
        response = await client.post(
            endpoint,
            files={"files": (form_name, file_bytes, ft.mime)},
        )
        response.raise_for_status()
        return response.content


# ---------------------------------------------------------------------------
# Step 5: PDF → Chunks
# ---------------------------------------------------------------------------


async def extract_pdf_chunks(sha256: str, pdf_bytes: bytes, file_meta: dict):
    doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
    page_chunks = pymupdf4llm.to_markdown(doc, page_chunks=True)

    file_meta.update(extract_pdf_meta(doc))
    has_pending = False
    all_links: list[str] = []

    for idx, page_data in enumerate(page_chunks):
        page_md = page_data["text"]
        page = doc[idx]

        # Linkify bare URLs and collect links from this page
        page_md = linkify_urls(page_md)
        all_links.extend(extract_links(page_md))

        has_images = len(page.get_images()) > 0

        if has_images:
            dpi = compute_page_dpi(page)
            pix = page.get_pixmap(dpi=dpi)
            jpg_bytes = pix.tobytes("jpeg", jpg_quality=85)
            chunk_hash_input = jpg_bytes + page_md.encode("utf-8")
            chunk_sha = hashlib.sha256(chunk_hash_input).hexdigest()
            await db.upsert_chunk(chunk_sha, page_md, pending=True)
            has_pending = True
        else:
            chunk_sha = hashlib.sha256(page_md.encode("utf-8")).hexdigest()
            await db.upsert_chunk(chunk_sha, page_md)

        await db.upsert_file_chunk(sha256, chunk_sha, idx)

    doc.close()

    file_meta["num_pages"] = len(page_chunks)
    if all_links:
        file_meta["links"] = all_links
    await db.update_file_meta(sha256, file_meta)

    if has_pending:
        # Save PDF to tmp so worker can render pages for OCR
        tmp_dir = "/tmp/chunkydonkey"
        os.makedirs(tmp_dir, exist_ok=True)
        with open(os.path.join(tmp_dir, f"{sha256}.pdf"), "wb") as f:
            f.write(pdf_bytes)

        # Set num_chunks but leave processed_at NULL — worker sets it when done
        async with db.pool().acquire() as conn:
            await conn.execute(
                "UPDATE files SET num_chunks = $1 WHERE sha256 = $2",
                len(page_chunks), sha256,
            )
    else:
        await db.set_processed(sha256, len(page_chunks))


def extract_pdf_meta(doc: pymupdf.Document) -> dict:
    meta = {}
    pdf_meta = doc.metadata
    if pdf_meta:
        for key in ("title", "author", "subject", "keywords", "creator",
                     "producer", "creationDate", "modDate"):
            val = pdf_meta.get(key, "")
            if val and val.strip():
                meta[key] = val.strip()
    return meta


# ---------------------------------------------------------------------------
# Step 6: Tabular → Chunks
# ---------------------------------------------------------------------------


async def extract_tabular_chunks(sha256: str, file_bytes: bytes, file_meta: dict):
    df = None

    if file_bytes[:4] == b"PAR1":
        df = pl.read_parquet(io.BytesIO(file_bytes))

    if df is None:
        try:
            df = pl.read_json(io.BytesIO(file_bytes))
        except Exception:
            try:
                df = pl.read_ndjson(io.BytesIO(file_bytes))
            except Exception:
                pass

    if df is None:
        try:
            text = file_bytes.decode("utf-8", errors="replace")
            sniffer = clevercsv.Sniffer()
            dialect = sniffer.sniff(text, verbose=False)
            if dialect and dialect.delimiter:
                has_header = sniffer.has_header(text)
                quote_char = dialect.quotechar if dialect.quotechar != "" else None
                df = pl.read_csv(
                    io.BytesIO(file_bytes),
                    separator=dialect.delimiter,
                    quote_char=quote_char,
                    has_header=has_header,
                    infer_schema_length=10000,
                    ignore_errors=True,
                    truncate_ragged_lines=True,
                )
        except Exception:
            pass

    if df is None or df.height == 0:
        await db.set_errored(sha256, "could not parse tabular data")
        return

    df = flatten_df(df)

    df = df.with_columns(
        pl.int_range(1, df.height + 1).alias("RowID")
    ).select(["RowID"] + [c for c in df.columns if c != "RowID"])

    df = df.with_columns([
        pl.col(c)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.replace_all(r"\s+", " ")
        .str.replace_all(r"\|", "¦")
        .str.strip_chars()
        .alias(c)
        for c in df.columns
    ])

    target_chunk_chars = 8000
    max_rows = min(100, df.height)
    chunks = []
    start = 0
    while start < df.height:
        chunk_df = df.slice(start, max_rows)
        md = tabular_to_markdown(chunk_df)
        if len(md) > target_chunk_chars and chunk_df.height > 1:
            max_rows = max(1, int(chunk_df.height * target_chunk_chars / len(md)))
            continue
        chunks.append(md)
        start += chunk_df.height
        max_rows = min(100, df.height - start)

    file_meta.update({"num_rows": df.height, "num_columns": len(df.columns), "columns": df.columns})
    await db.update_file_meta(sha256, file_meta)

    for idx, md in enumerate(chunks):
        chunk_sha = hashlib.sha256(md.encode("utf-8")).hexdigest()
        await db.upsert_chunk(chunk_sha, md)
        await db.upsert_file_chunk(sha256, chunk_sha, idx)

    await db.set_processed(sha256, len(chunks))


# ---------------------------------------------------------------------------
# Step 7: Image → Single chunk (treated as scanned document)
# ---------------------------------------------------------------------------


async def extract_image_chunk(sha256: str, image_bytes: bytes, file_meta: dict):
    """Treat an image as a single-page scanned document. Sent to VLM worker for OCR."""
    await db.update_file_meta(sha256, file_meta)

    placeholder = "![image](image)"
    chunk_hash_input = image_bytes + placeholder.encode("utf-8")
    chunk_sha = hashlib.sha256(chunk_hash_input).hexdigest()
    await db.upsert_chunk(chunk_sha, placeholder, pending=True)
    await db.upsert_file_chunk(sha256, chunk_sha, 0)

    # Save original image to tmp so worker can read it
    tmp_dir = "/tmp/chunkydonkey"
    os.makedirs(tmp_dir, exist_ok=True)
    with open(os.path.join(tmp_dir, f"{chunk_sha}.img"), "wb") as f:
        f.write(image_bytes)

    async with db.pool().acquire() as conn:
        await conn.execute(
            "UPDATE files SET num_chunks = 1 WHERE sha256 = $1", sha256,
        )


def flatten_df(df: pl.DataFrame) -> pl.DataFrame:
    while any(isinstance(dtype, (pl.Struct, pl.List)) for dtype in df.dtypes):
        for col in df.columns:
            if isinstance(df[col].dtype, pl.Struct):
                unnested = df[col].struct.unnest()
                unnested = unnested.rename({c: f"{col}.{c}" for c in unnested.columns})
                df = df.drop(col).hstack(unnested)
            elif isinstance(df[col].dtype, pl.List):
                prim_type = df[col].dtype.inner.is_numeric() or df[col].dtype.inner == pl.Boolean
                max_len_4 = (df[col].list.len().max() or 0) > 4
                if prim_type and max_len_4:
                    df = df.drop(col)
                else:
                    df = df.with_columns(
                        pl.col(col)
                        .map_elements(stringify_list_value, return_dtype=pl.Utf8)
                        .alias(col)
                    )
    return df


def stringify_list_value(values) -> str:
    if values is None:
        return ""
    return ", ".join("" if value is None else str(value) for value in values)


def tabular_to_markdown(df: pl.DataFrame) -> str:
    lines = [" | ".join(df.columns)]
    for row in df.iter_rows():
        cells = [format_tabular_cell(value) for value in row]
        lines.append(" | ".join(cells))
    return "\n".join(lines)


def format_tabular_cell(value) -> str:
    if value is None:
        return ""
    return str(value).replace("\r", " ").replace("\n", " ")
