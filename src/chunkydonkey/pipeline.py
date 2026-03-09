import asyncio
import filetype
import hashlib

from . import db
from . import utils
from . import storage
from .archive_to_files import archive_to_files
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
    system_meta: 
    url: str | None = None,
    file: bytes | None = None,
    use_cache: bool = True,
):

    # URL route
    if url and not file:
        file, resolved_url = await url_to_file(url)



        unarchived = await parse_from_archive(file_bytes, file_name)
    if unarchived:
        return [
            parsed
            for file in unarchived
            for parsed in await parse(file['file_bytes'], file['file_name'], use_cache)
        ]

    # Cache route
    if use_cache:
        if url:
            sha256 = db.
        
        
         and (cache := await db.get_from_source(source, source_id)):
        return cache

    # Generate hash and touch record
    sha256 = hashlib.sha256(file or url.encode("utf-8")).hexdigest()
    await db.upsert_source(source, source_id, sha256, source_meta)

    # Generate image_source and touch related images
    image_source = f"images/{source}/{source_id}"
    await db.touch_source(image_source)

    # Cache route (game over)
    if use_cache and (cache := await db.get_file(sha256)):
        return cache

    # Save original file
    storage.save(sha256, file or url.encode("utf-8"))


    # HOV HOV - TANKE HER - det kan være svært at finde ud af url vs fil med sha256 i storage save osv ... tænker lige igennem - to sek

    # VI AHR LØST DET MED url_file TABEL ... så 


    # Init meta
    meta = {}
    base_url = None

    # URL route
    if url and not file:
        file, images, base_url = await url_to_file(url)
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))

    # Save original file
    storage.save(sha256, file)

    # Init filetype and meta
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
        file, images, html_meta = html_to_markdown(file, base_url=base_url)
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))
        meta = html_meta | meta
    except Exception:
        pass

    # Markdown route (including plain text)
    try:
        chunks, images, markdown_meta = markdown_to_chunks(file)
        for image_sha256, image_file in images.items():
            asyncio.create_task(process(source=image_source, source_id=image_sha256, file=image_file))
        meta = markdown_meta | meta
    except Exception:
        pass

    # Unknown/Error route
