import asyncio
import filetype
import hashlib

from . import db
from . import utils
from . import storage
from .url_to_file import url_to_file
from .archive_to_files import archive_to_files
from .image_to_markdown import image_to_markdown
from .office_to_pdf import office_to_pdf
from .pdf_to_markdown import pdf_to_markdown
from .tabular_to_markdown import tabular_to_markdown
from .html_to_markdown import html_to_markdown
from .markdown_to_markdown import markdown_to_markdown


async def process(
    source: str | None,
    source_id: str | None,
    file: bytes | None,
    meta: dict | None,
    parent: str | None,
    max_age: int = 86400, # 24 hrs default
):

    # URL route, if no file given (crash loudly if no URL either)
    if not file:
        sha256 = await db.get_sha256_from_url(meta.get('url'), max_age)
        if not sha256:
            file, resolved_url = await url_to_file(meta.get('url'))
            sha256 = hashlib.sha256(file).hexdigest()
            await db.upsert_url(meta.get('url'), resolved_url, sha256)
    else:
        meta.pop('url', None) # ensure no URL when file to avoid confusion
        sha256 = hashlib.sha256(file).hexdigest()

    # Upsert source relation, if directly from client
    if source and source_id:
        await db.upsert_source(
            source=source,
            source_id=source_id,
            file_sha256=sha256,
            meta=meta,
        )

    # Upsert parent relation, if child (e.g. from archive or embedded image)
    if parent:
        await db.upsert_parent_relation(
            parent_sha256=parent,
            child_sha256=sha256,
            meta=meta
        )

    # Cache route, if file already processed into meta and chunks
    if (cache := await db.get_file_from_sha256(sha256)):
        return cache

    # Guess filetype
    ft = await asyncio.to_thread(filetype.guess, file)

    # Archive route, unzip before cache, upsert archive file, process children
    if ft and ft.extension in ('gz', 'zip', 'tar'):
        child_files, meta = await asyncio.to_thread(archive_to_files, file)
        if child_files:
            processed = await asyncio.gather(*[
                process(
                    source=None,
                    source_id=None,
                    file=child[1],
                    meta={'index': 'parent', 'filename': child[0]},
                    parent=sha256,
                )
                for child in child_files
            ])
            return await finalize(sha256, meta)

    # Save original file (re-saved with correct content_type in finalize)
    await storage.save(sha256, file)

    # Init accumulative file meta
    meta = {}

    # Image route
    if ft and ft.mime[:6] == 'image/':
        markdown, image_meta = await image_to_markdown(file, ft)
        meta = image_meta | meta
        return await finalize(sha256, meta, markdown, file=file)

    # Office route (rich text, presentations, spreadsheets)
    if ft and ft.extension in ('doc', 'docx', 'ppt', 'pptx', 'odp', 'xls', 'xlsx', 'ods'):
        file, office_meta = await office_to_pdf(file, ft)
        meta = office_meta | meta

    # Refresh filetype if changed
    ft = await asyncio.to_thread(filetype.guess, file)

    # PDF route
    if ft and ft.extension == 'pdf':
        markdown, images, pdf_meta = await asyncio.to_thread(pdf_to_markdown, file)
        process_images(images, sha256, max_age)
        meta = pdf_meta | meta
        return await finalize(sha256, meta, markdown, file=file)

    # Ensure uniform encoding and line separation, if text
    file = await asyncio.to_thread(utils.normalize_text, file)

    # Tabular route (parquet, json, csv)
    try:
        markdown, tabular_meta = await asyncio.to_thread(tabular_to_markdown, file)
        meta = tabular_meta | meta
        return await finalize(sha256, meta, markdown, file=file)
    except Exception:
        pass

    # HTML route
    try:
        file, images, html_meta = await asyncio.to_thread(html_to_markdown, file)
        process_images(images, sha256, max_age)
        meta = html_meta | meta
    except Exception:
        pass

    # Markdown route (any utf-8-decodable txt)
    try:
        markdown, images, markdown_meta = await asyncio.to_thread(markdown_to_markdown, file)
        process_images(images, sha256, max_age)
        meta = markdown_meta | meta
        return await finalize(sha256, meta, markdown, file=file)
    except Exception:
        pass

    # Unknown/Error route
    return await finalize(sha256, {'content_type': 'unknown'})


def process_images(images: set, sha256: str, max_age: int):
    for image in images:
        asyncio.create_task(
            process(
                source=None,
                source_id=None,
                file=image if isinstance(image, bytes) else None,
                meta={'index': 'child'} | ({'url': image} if isinstance(image, str) else {}),
                parent=sha256,
                max_age=max_age,
            )
        )


async def finalize(sha256: str, meta: dict, markdown: str | None = None, file: bytes | None = None):
    if markdown:
        markdown = utils.linkify_urls(markdown)
        links = utils.extract_urls(markdown)
        if links:
            meta['links'] = links
    # Update file with correct content type
    if file:
        await storage.save(sha256, file, meta.get('content_type', 'application/octet-stream'))
    # Or delete file if archive or unknown
    else:
        await storage.delete(sha256)
    await db.upsert_file(sha256, meta, markdown)
    
    return await db.get_file_from_sha256(sha256)
