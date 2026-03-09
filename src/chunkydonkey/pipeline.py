import asyncio
import filetype
import hashlib

from . import db
from . import utils
from .url_to_file import url_to_file
from .image_to_ocr import image_to_ocr
from .office_to_pdf import office_to_pdf
from .pdf_to_chunks import pdf_to_chunks
from .tabular_to_chunks import tabular_to_chunks
from .html_to_markdown import html_to_markdown


async def process(
    source: str,
    source_id: str,
    source_meta: dict | None = None,
    url: str | None = None,
    file: bytes | None = None,
    use_cache: bool = True,
):
    
    # Generate hash and touch record
    sha256 = hashlib.sha256(file or url.encode("utf-8")).hexdigest()
    await db.upsert_source(source, source_id, sha256, source_meta)

    # Generate image_source and touch related images
    image_source = f"images/{source}/{source_id}"
    await db.touch_source(image_source)

    # Cache route (game over)
    if use_cache and (cache := await db.get_file(sha256)):
        return cache

    # No cache, save to /tmp while processing
    utils.tmp_save(sha256, file or url.encode("utf-8"))

    # Init meta
    meta = {}

    # URL route
    if url and not file:
        file, images = await url_to_file(url)
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))

    # Init filetype and meta
    ft = filetype.guess(file)

    # Image route
    if ft.mime[:6] == 'image/':
        chunks, image_meta = image_to_ocr(file)
        meta = image_meta | meta | {'content_type': ft.mime}
        await update_image_chunks(sha256, chunks[0])            # update chunks from other files which reference this image
        await update(sha256, chunks, meta)                      # update file and related chunk (images always have just one)
        return await db.get_file(sha256)                        # return

    # Office route (rich text, presentations, spreadsheets)
    if ft.extension in ('doc', 'docx', 'ppt', 'pptx', 'odp', 'xls', 'xlsx', 'ods'):
        file, office_meta = office_to_pdf(file, ft)
        meta = office_meta | meta | {'content_type': ft.mime}

    # Refresh filetype if changed
    ft = filetype.guess(file)

    # PDF route
    if ft.extension == 'pdf':
        chunks, images, pdf_meta = pdf_to_chunks(file)
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))
        meta = pdf_meta | {'content_type': ft.mime} | meta
        await update(sha256, chunks, meta)                      # update file and related chunks
        return await db.get_file(sha256)                        # return

    # Ensure uniform encoding and line separation
    file = utils.normalize_text(file)

    # Tabular route (parquet, json, csv)
    try:
        chunks, tabular_meta = await tabular_to_chunks(file)
        meta = tabular_meta | meta                              # content_type included in tabular_meta
    except Exception:
        pass

    # HTML route
    try:
        file, images, html_meta = html_to_markdown(file, base_url=url)
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))
        meta = html_meta | meta                                 # content_type included in html_meta
    except Exception:
        pass

    # Markdown route (including plain text)
    try:
        chunks, images = markdown_to_chunks(file)
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))
    except Exception:
        pass

    # Unknown/Error route
