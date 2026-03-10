import asyncio
import filetype
import hashlib

from . import db
from . import utils
from . import storage
from .image_to_ocr import image_to_ocr
from .office_to_pdf import office_to_pdf
from .pdf_to_chunks import pdf_to_chunks
from .tabular_to_chunks import tabular_to_chunks
from .html_to_markdown import html_to_markdown


async def process(
    source: str,
    source_id: str,
    source_meta: dict | list,
    system_meta: dict,
    file: bytes,
    use_cache: bool = True,
):

    # Generate hash and touch record
    sha256 = hashlib.sha256(file).hexdigest()
    await db.upsert_source(source, source_id, source_meta, system_meta, sha256)

    # FIX DET HER IMAGE SOURCE OG EFTERFØLGENDE
    # Generate image_source and touch related images
    image_source_id = f"{source}/{source_id}"
    await db.touch_source(image_source)

    # Cache route
    if use_cache and (cache := await db.get_cache(sha256)):
        return cache

    # Save original
    storage.save(sha256, file)

    # Init filetype and meta
    meta = {}
    ft = filetype.guess(file)

    # Image route
    if ft.mime[:6] == 'image/':
        chunks, image_meta = image_to_chunks(file, ft)
        meta = image_meta | meta
        await update_image_chunks(sha256, chunks[0])            # update chunks from other files which reference this image
        await update(sha256, chunks, meta)                      # update file and related chunk (images always have just one)
        return await db.get_file(sha256)                        # return

    # Office route (rich text, presentations, spreadsheets)
    if ft.extension in ('doc', 'docx', 'ppt', 'pptx', 'odp', 'xls', 'xlsx', 'ods'):
        file, office_meta = office_to_pdf(file, ft)
        meta = office_meta | meta

    # Refresh filetype if changed
    ft = filetype.guess(file)

    # PDF route
    if ft.extension == 'pdf':
        chunks, images, pdf_meta = pdf_to_chunks(file)
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))
        meta = pdf_meta | meta
        await update(sha256, chunks, meta)                      # update file and related chunks
        return await db.get_file(sha256)                        # return

    # Ensure uniform encoding and line separation
    file = utils.normalize_text(file)

    # Tabular route (parquet, json, csv)
    try:
        chunks, tabular_meta = await tabular_to_chunks(file)
        meta = tabular_meta | meta
    except Exception:
        pass

    # HTML route
    try:
        file, images, html_meta = html_to_markdown(file, base_url=system_meta.get('resolved_url') or system_meta.get('url'))
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))
        meta = html_meta | meta
    except Exception:
        pass

    # Markdown route (including plain text)
    try:
        chunks, images, markdown_meta = markdown_to_chunks(file, base_url=system_meta.get('resolved_url') or system_meta.get('url'))
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))
        meta = markdown_meta | meta
    except Exception:
        pass

    # Unknown/Error route
