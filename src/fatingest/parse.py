# src/fatingest/parse.py

import asyncio
import clevercsv
import filetype
import glob
import gzip
import httpx
import io
import logging
import markdown as md
import math
import os
import polars as pl
import re
import tarfile
import tempfile
import zipfile

from .blobstorage import BlobStorage
from .vlm import image_to_text
from typing import BinaryIO


MARKDOWN_CACHE_BUCKET = "markdown-cache"


logger = logging.getLogger()
storage = BlobStorage()


async def init_parse():
    """
    Initialize storage bucket for markdown cache.
    """
    await storage.ensure_bucket(MARKDOWN_CACHE_BUCKET)


async def parse(file_bytes: bytes | BinaryIO, file_name: str, use_cache: bool = True) -> list[dict]:
    """
    Initiates parsing from file bytes and name, leveraging cache unless specified otherwise.
    Supports archives, audio/video, tabular, images, documents and text formats.
    Returns a list of any file(s) that was parsed into markdown in the format:
        [
            {
                'file_name': 'document.pdf',
                'content_type': 'application/pdf',
                'markdown': '...',
                'file_key': '/ab/cd/efgh...',
            }
            ...
        ]
    """
    # Normalize bytes
    file_bytes = file_bytes.read() if isinstance(file_bytes, BinaryIO) else file_bytes
    # Get sha256-based CAS key as unique identifier and test for cache
    file_key = await storage.key(file_bytes)
    if use_cache:
        cached = await parse_from_cache(file_key)
        if cached:
            cached |= {'file_key': file_key}
            return [cached]
    # Test for archive and recursively unarchive
    unarchived = await parse_from_archive(file_bytes, file_name)
    if unarchived:
        return [
            parsed
            for file in unarchived
            for parsed in await parse(file['file_bytes'], file['file_name'], use_cache)
        ]
    # Parse non-cached, non-archive file
    parsed = await parse_to_markdown(file_bytes, file_name)
    parsed |= {'file_key': file_key}
    await parse_to_cache(parsed)
    return [parsed]


async def parse_from_archive(file_bytes: bytes, file_name: str) -> list[dict]:
    """
    Will unarchive zip, tar and gzip bytes or return empty.
    Returns a list of any files that was unarchived in the format:
        [{'file_bytes': '...', 'file_name': 'document.pdf'}]
    """
    file_ext = filetype.guess_extension(file_bytes)
    extracted = []
    if file_ext == 'zip':
        try:
            with zipfile.ZipFile(io.BytesIO(file_bytes)) as z:
                for name in z.namelist():
                    if not z.getinfo(name).is_dir():  # Files only
                        extracted.append({
                            'file_bytes': z.read(name),
                            'file_name': name,
                        })
        except Exception as e:
            logger.warning(f"Failed unarchiving {file_name}: {e}")
            pass
    elif file_ext == 'tar':
        try:
            with tarfile.open(fileobj=io.BytesIO(file_bytes)) as t:
                for member in t:
                    if member.isfile():  # Files only
                        f = t.extractfile(member)
                        if f:
                            extracted.append({
                                'file_bytes': f.read(),
                                'file_name': member.name,
                            })
        except Exception as e:
            logger.warning(f"Failed unarchiving {file_name}: {e}")
            pass
    elif file_ext == 'gz':
        try:
            extracted = [{
                'file_bytes': gzip.decompress(file_bytes),
                'file_name': file_name,
            }]
        except Exception as e:
            logger.warning(f"Failed unarchiving {file_name}: {e}")
            pass
    return extracted


async def parse_from_cache(file_key: str) -> dict | None:
    """
    Retrieves cached markdown from given file key.
    """
    cache = await storage.get(MARKDOWN_CACHE_BUCKET, file_key)
    if cache:
        return {
            'file_name': cache['file_name'],
            'content_type': cache['content_type'],
            'markdown': cache['file_bytes'].decode('utf-8')
        }
    return None


async def parse_to_cache(parsed: dict):
    """
    Caches parsed markdown and returns file keys.
    """
    await storage.put(
        MARKDOWN_CACHE_BUCKET,
        parsed['markdown'].encode('utf-8'),
        parsed['file_name'],
        parsed['content_type'],
        parsed.get('file_key')
    )


async def parse_to_markdown(file_bytes: bytes, file_name: str) -> dict:
    """
    Parses given file and returns markdown.
    Supports audio/video, tabular, images, documents and text formats.
    Returns a dict in the format:
        {'file_name': 'document.pdf', 'content_type': 'application/pdf', 'markdown': '...'}
    """
    file_type = filetype.guess(file_bytes)

    # Detection-based
    if file_type:

        # Audio/video
        if file_type.mime[:6] in ('audio/', 'video/'):
            markdown = await audio_to_markdown(file_bytes, file_name, file_type.mime)
            return {'file_name': file_name, 'content_type': file_type.mime, 'markdown': markdown}

        # Image
        if file_type.mime[:6] == 'image/':
            markdown = await image_to_markdown(file_bytes, file_name)
            return {'file_name': file_name, 'content_type': file_type.mime, 'markdown': markdown}

        # Spreadsheet
        if file_type.extension in ('xls', 'xlsx', 'ods'):
            markdown = await spreadsheet_to_markdown(file_bytes, file_name, file_type.extension)
            return {'file_name': file_name, 'content_type': file_type.mime, 'markdown': markdown}

        # Document
        if file_type.extension in ('pdf', 'doc', 'docx', 'ppt', 'pptx', 'odp'):
            markdown = await document_to_markdown(file_bytes, file_name, file_type.extension, file_type.mime)
            return {'file_name': file_name, 'content_type': file_type.mime, 'markdown': markdown}
    
    # Duck-based
    else:

        # Parquet
        markdown = await parquet_to_markdown(file_bytes)
        if markdown:
            return {'file_name': file_name, 'content_type': 'application/vnd.apache.parquet', 'markdown': markdown}

        # Ensure normalized text encoding and EOL before testing for various text-based formats
        file_bytes = await normalize_text(file_bytes)

        # JSON
        markdown = await json_to_markdown(file_bytes)
        if markdown:
            return {'file_name': file_name, 'content_type': 'application/json', 'markdown': markdown}
        
        # CSV
        markdown = await csv_to_markdown(file_bytes)
        if markdown:
            return {'file_name': file_name, 'content_type': 'text/csv', 'markdown': markdown}

        # Text/markdown/HTML
        markdown = await text_to_markdown(file_bytes, file_name, 'html')
        if markdown:
            return {'file_name': file_name, 'content_type': 'text/plain', 'markdown': markdown}

    # Empty
    return {'file_name': file_name, 'content_type': 'unknown/unsupported', 'markdown': ''}


async def text_to_markdown(file_bytes: bytes, file_name: str, file_ext: str) -> str:
    """
    Attemps to open file and normalize any html or markdown mix to clean markdown.
    """
    try: # Text-based?
        text = file_bytes.decode('utf-8')
        text = md.markdown(text)
        # UNFINISHED
        markdown = "" # await document_to_markdown(text.encode('utf-8'), file_name, file_ext)
    except:
        return ""
    return markdown


async def spreadsheet_to_markdown(file_bytes: bytes, file_name: str, file_ext: str) -> str:
    """
    Attempts to open file as Parquet in Polars and returns markdown.
    """
    try: # Spreadsheet?
        if file_ext == 'ods':
            df = pl.read_ods(io.BytesIO(file_bytes))
        else:
            df = pl.read_excel(io.BytesIO(file_bytes))
    except Exception as e:
        logger.warning(f"Failed converting spreadsheet to markdown {file_name}: {e}")
        return ""
    return await df_to_markdown(df)


async def parquet_to_markdown(file_bytes: bytes) -> str:
    """
    Attempts to open file as Parquet in Polars and returns markdown.
    """
    try: # Parquet?
        df = pl.read_parquet(io.BytesIO(file_bytes))
    except:
        return ""
    return await df_to_markdown(df)


async def json_to_markdown(file_bytes: bytes) -> str:
    """
    Attempts to open file as JSON in Polars and returns markdown.
    """
    try: # Regular JSON?
        df = pl.read_json(io.BytesIO(file_bytes))
    except:
        try: # NDJSON?
            df = pl.read_ndjson(io.BytesIO(file_bytes))
        except:
            return ""
    return await df_to_markdown(df)


async def csv_to_markdown(file_bytes: bytes) -> str:
    """
    Attempts to detect and open file as CSV in polars and returns markdown.
    """
    try:
        # Decode and detect dialect (delimiter, quotechar, escapechar)
        text = file_bytes.decode('utf-8', errors='replace')
        sniffer = clevercsv.Sniffer()
        dialect = sniffer.sniff(text, verbose=False)
        if dialect is None or dialect.delimiter is None:
            return ""
        # Detect if file has header
        has_header = sniffer.has_header(text)
        # Map CleverCSV dialect to Polars parameters ('' -> None)
        quote_char = dialect.quotechar if dialect.quotechar != '' else None
        # Read CSV with Polars using detected dialect
        df = pl.read_csv(
            io.BytesIO(file_bytes),
            separator=dialect.delimiter,
            quote_char=quote_char,
            has_header=has_header,
            infer_schema_length=10000,   # Good balance of speed/accuracy
            ignore_errors=True,          # Skip malformed rows
            truncate_ragged_lines=True,  # Handle uneven row lengths
        )
    except Exception:
        return ""
    return await df_to_markdown(df)


async def df_to_markdown(df: pl.DataFrame) -> str:
    """
    Returns df flattened and exported to markdown.
    """
    # Flatten structs and stringify lists (drop primitive lists of len > 4)
    while any(isinstance(dtype, pl.Struct) or isinstance(dtype, pl.List) for dtype in df.dtypes):
        for col in df.columns:
            # Flatten structs
            if isinstance(df[col].dtype, pl.Struct):
                unnested = df[col].struct.unnest()
                unnested = unnested.rename({c: f"{col}.{c}" for c in unnested.columns})
                df = df.drop(col).hstack(unnested)
            elif isinstance(df[col].dtype, pl.List):
                # Drop primitive lists of len > 4
                prim_type = df[col].dtype.inner.is_numeric() or df[col].dtype.inner == pl.Boolean # type: ignore
                max_len_4 = (df[col].list.len().max() or 0) > 4 # type: ignore
                if prim_type and max_len_4:
                    df = df.drop(col)
                # Stringify remaining lists
                else:
                    df = df.with_columns(pl.col(col).list.join(", "))
    # Add row index column
    df = df.with_columns(pl.int_range(1, df.height + 1).alias("RowID")).select(
        ["RowID"] + df.columns
    )
    # Sanitize
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
    return df.to_pandas().to_markdown(index=False)


async def audio_to_markdown(file_bytes: bytes, file_name: str, content_type: str) -> str:
    """
    Transcribes audio/video using Whisper and returns markdown table.
    """
    try:
        # Run Whisper
        endpoint = os.environ.get("WHISPER_URL", "http://whisper:8000").rstrip("/")
        async with httpx.AsyncClient(timeout=None) as client:
            response = await client.post(
                f"{endpoint}/v1/audio/transcriptions",
                files={'file': (file_name, file_bytes, content_type)},
                data={
                    'response_format': 'verbose_json',
                    'hallucination_silence_threshold': '2.0',
                    #'vad_filter': 'true', # too many hallucinations :(
                },
            )
            response.raise_for_status()
            result = response.json()
        # Retrieve segments
        segments = result.get('segments', [])
        if not segments:
            logger.warning(f"No segments returned from Whisper for {file_name}")
            return ""
        # Merge segments adaptively to minute boundaries
        merged = merge_to_adaptive_minutes(segments, min_duration=30.0)
        # Build data frame
        df = pl.DataFrame({
            "Time": [f"{format_timestamp(s['start'])} - {format_timestamp(s['end'])}" for s in merged],
            "Speech": [s["text"] for s in merged],
        })
    except httpx.HTTPError as e:
        logger.warning(f"HTTP error transcribing {file_name}: {e}")
        return ""
    except Exception as e:
        logger.warning(f"Failed transcribing audio {file_name}: {e}")
        return ""
    return await df_to_markdown(df)


def merge_to_adaptive_minutes(segments: list[dict], min_duration: float = 30.0, interval: float = 60.0) -> list[dict]:
    """
    Merges Whisper segments adaptively:
    - Always merge until exceeding next whole minute from buffer start
    - After flush, next buffer must be min_duration AND exceed the next whole minute after that
    """
    merged = []
    buffer_segments = []
    buffer_start = 0.0
    for seg in segments:
        buffer_segments.append(seg)
        buffer_end = seg['end']
        buffer_duration = buffer_end - buffer_start
        # Next whole minute from buffer start
        next_minute_from_start = math.ceil(buffer_start / interval) * interval
        # Have we exceeded that minute?
        if buffer_end > next_minute_from_start:
            # Do we also have minimum duration?
            if buffer_duration >= min_duration:
                # What's the next whole minute after (start + min_duration)?
                min_end = buffer_start + min_duration
                next_minute_after_min = math.ceil(min_end / interval) * interval
                # Have we exceeded that minute too?
                if buffer_end >= next_minute_after_min:
                    # Flush buffer
                    merged_seg = {
                        'start': buffer_start,
                        'end': buffer_end,
                        'text': ' '.join(s['text'].strip() for s in buffer_segments)
                    }
                    merged.append(merged_seg)
                    # Reset for next buffer
                    buffer_start = buffer_end
                    buffer_segments = []
    # Flush remaining
    if buffer_segments:
        buffer_end = buffer_segments[-1]['end']
        merged_seg = {
            'start': buffer_start,
            'end': buffer_end,
            'text': ' '.join(s['text'].strip() for s in buffer_segments)
        }
        merged.append(merged_seg)
    return merged


def format_timestamp(seconds: float) -> str:
    """
    Formats seconds to HH:MM:SS timestamp.
    """
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h:02d}:{m:02d}:{s:02d}"
 

async def normalize_text(file_bytes: bytes) -> bytes:
    """
    If given text bytes, will encourage utf-8 and \n EOL.
    Returns bytes.
    """
    for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
        try:
            text = file_bytes.decode(encoding)
            text = text.replace('\r\n', '\n').replace('\r', '\n')
            return text.encode('utf-8')
        except:
            continue
    return file_bytes


async def normalize_audio(file_bytes: bytes, file_name: str) -> bytes:
    """
    Accepts (almost) any audio or video format and returns WAV16 bytes.
    """
    cmd = [
        'ffmpeg',
        '-hide_banner',
        '-nostdin',
        '-i', 'pipe:0',                     # Read input from stdin
        '-vn',                              # Discard video
        '-acodec', 'pcm_s16le',             # Audio Codec: PCM 16-bit (WAV standard)
        '-ar', '16000',                     # Audio Rate: 16 kHz (Whisper standard)
        '-channel_layout', 'mono',          # Mono layout explicited
        '-ac', '1',                         # Audio Channels: 1 (Mono)
        '-sample_fmt', 's16',               # Sample format explicited
        '-af', 'aresample=resampler=soxr',  # Soxr resampler
        '-f', 'wav',                        # Format: WAV
        'pipe:1'                            # Write to stdout
    ]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate(input=file_bytes)
        if process.returncode != 0:
            process.kill()
            await process.wait()
            error_msg = stderr.decode('utf-8', errors='ignore')
            logger.warning(f"ffmpeg failed for {file_name} (code {process.returncode}): {error_msg}")
            return b""
        return stdout
    except Exception as e:
        logger.warning(f"Failed normalizing audio {file_name}: {e}")
        return b""


async def normalize_image(file_bytes: bytes, file_name: str, max_w: int = 2048, max_h: int = 2048) -> bytes:
    """
    Accepts (almost) any image format and returns PNG bytes.
    """
    cmd = [
        'ffmpeg',
        '-hide_banner',
        '-nostdin',
        '-i', 'pipe:0',                     # Read from stdin
        '-map', '0:v:0',                    # Get first stream (video)
        '-frames:v', '1',                   # Get 1 frame only (for GIF/video)
        '-vf', f"scale='min({max_w},iw)':'min({max_h},ih)':force_original_aspect_ratio=decrease",
        '-f', 'image2pipe',                 # Send image-bytes to pipe
        '-vcodec', 'png',                   # Output as PNG
        '-pix_fmt', 'rgb24',                # 8-bit, no alpha
        'pipe:1'                            # Write to stdout
    ]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(input=file_bytes),
                timeout=10,
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            logger.warning(f"ffmpeg timeout for {file_name}")
            return b""
        if process.returncode != 0:
            error_msg = stderr.decode("utf-8", errors="ignore")
            logger.warning(f"ffmpeg failed for {file_name} (code {process.returncode}): {error_msg}")
            return b""
        return stdout
    except Exception as e:
        logger.warning(f"Failed normalizing image {file_name}: {e}")
        return b""


async def document_to_markdown(
    file_bytes: bytes,
    file_name: str,
    file_ext: str,
    content_type: str,
    page_interval: int = 25
) -> str:
    """
    Attempts to parse document through PDF > PNG > VLM and returns markdown.
    """
    if file_ext == 'pdf':
        pdf_bytes = file_bytes
    else:
        pdf_bytes = await document_to_pdf(file_bytes, file_name, file_ext, content_type)
    if not pdf_bytes:
        return ""
    num_pages = await pdf_num_pages(pdf_bytes, file_name)
    if not num_pages:
        return ""
    # parse pages in chunks
    chunks: list[str] = []
    for start in range(1, num_pages + 1, page_interval):
        end = min(start + page_interval - 1, num_pages)
        images = await pdf_to_images(
            pdf_bytes,
            file_name,
            f=start,
            l=end,
            max_h=2048
        )
        if not images:
            continue
        # OCR each page image
        for idx, image_bytes in enumerate(images, start=start):
            markdown = await image_to_markdown(image_bytes, f"{file_name}#p{idx}")
            chunks.append(f"<!--page:{idx}-->")
            chunks.append(markdown)
    return "\n\n".join(chunks)


async def pdf_num_pages(file_bytes: bytes, file_name: str, timeout: int = 5) -> int:
    """
    Attempts to determine the number of pages in given pdf file, or 0 if unsuccessful.
    """
    cmd = ["pdfinfo", "-"]
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(input=file_bytes),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            logger.warning(f"pdfinfo timeout for {file_name}")
            return 0
        if process.returncode != 0:
            logger.warning(
                f"pdfinfo failed for {file_name} (code {process.returncode}): "
                f"{stderr.decode('utf-8', errors='ignore')}"
            )
            return 0
        text = stdout.decode("utf-8", errors="ignore")
        m = re.search(r"^Pages:\s+(\d+)\s*$", text, re.MULTILINE)
        return int(m.group(1)) if m else 0
    except Exception as e:
        logger.warning(f"Failed reading PDF pages for {file_name}: {e}")
        return 0


async def pdf_to_images(
    file_bytes: bytes,
    file_name: str,
    f: int | None = None,
    l: int | None = None,
    max_w: int | None = None, 
    max_h: int | None = None, 
    timeout: int = 30
) -> list[bytes]:
    """
    Converts given PDF bytes to a list of per-page PNG bytes using pdftoppm.
    """
    try:
        with tempfile.TemporaryDirectory(prefix="pdftoppm_") as tmpdir:
            out_prefix = os.path.join(tmpdir, "page")
            cmd = ["pdftoppm"]
            if f is not None:
                cmd += ["-f", str(f)]
            if l is not None:
                cmd += ["-l", str(l)]
            cmd += ["-png"]
            if max_h is not None:
                cmd += ["-scale-to-y", str(max_h)]
            if max_w is not None:
                cmd += ["-scale-to-x", str(max_w)]
            cmd += [
                "-",         # Read PDF from stdin
                out_prefix,  # Output prefix for file names
            ]
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,  # unused, but capture anyway
                stderr=asyncio.subprocess.PIPE
            )
            try:
                _, stderr = await asyncio.wait_for(
                    process.communicate(input=file_bytes),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                logger.warning(f"pdftoppm timeout for {file_name}")
                return []
            if process.returncode != 0:
                error_msg = stderr.decode("utf-8", errors="ignore")
                logger.warning(f"pdftoppm failed for {file_name} (code {process.returncode}): {error_msg}")
                return []
            # Collect and sort output images by page number
            paths = glob.glob(os.path.join(tmpdir, "page-*.png"))
            def page_num(path: str) -> int:
                m = re.search(r"page-(\d+)\.png$", path)
                return int(m.group(1)) if m else 10**9
            paths.sort(key=page_num)
            images: list[bytes] = []
            for p in paths:
                with open(p, "rb") as i:
                    images.append(i.read())
            return images
    except Exception as e:
        logger.warning(f"Failed converting PDF to images {file_name}: {e}")
        return []


async def document_to_pdf(file_bytes: bytes, file_name: str, file_ext: str, content_type: str) -> bytes | None:
    """
    Attempts to convert document (doc, docx, ppt, pptx, odp, odt) to pdf using Gotenberg, returning bytes or None.
    """
    endpoint = os.environ.get("GOTENBERG_URL", "http://gotenberg:3000").rstrip("/")
    form_name = file_name if file_name else f"document.{file_ext}"
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
            response = await client.post(
                f"{endpoint}/forms/libreoffice/convert",
                files={'files': (form_name, file_bytes, content_type)},
            )
            response.raise_for_status()
            return response.content
    except httpx.TimeoutException as e:
        logger.warning(f"Timeout converting {file_name} to PDF: {e}")
        return None
    except httpx.HTTPStatusError as e:
        logger.warning(f"HTTP {e.response.status_code} converting {file_name} to PDF: {e.response.text[:200]}")
        return None
    except Exception as e:
        logger.warning(f"Failed converting {file_name} to PDF: {e}")
        return None


async def image_to_markdown(file_bytes: bytes, file_name: str) -> str:
    """
    Attempts to perform OCR and/or image description using VLM and returns markdown.
    """
    file_bytes = await normalize_image(file_bytes, file_name)
    if not file_bytes:
        return ""
    prompt = (
        "Udfør perfekt OCR på den givne side og returner meningsfuldt indhold formatteret som markdown, intet andet.\n"
        "Organiser rækkefølgen af indholdet, så det respekterer flowets retning i diagrammer og tabulære relationer.\n"
        #"Ophæv brudte linjer og delte ord.\n"
        #"Anvend overskrifter, GFM-tabeller og lister for at strukturere indholdet, og ophæv brudte linjer og delte ord.\n"
        "Suppler med ![beskrivelse](image), når billeder kræver uddybning for at forstå sidens indhold.\n"
        #"Udfør komplet OCR for at konvertere indholdet fra den givne side til præcis markdown-syntaks, intet andet.\n"
        #"Respekter sidens visuelle flow og læseretning.\n"
        #"Indsæt en ekstra ![kort beskrivelse](image:[1...n]) efter teksten, når en illustration kræver en ekstra beskrivelse for at forstå sidens indhold.\n"
        #"Udelad ugenkendeligt og uforståeligt indhold.\n"
        "Vær opmærksom på, at indholdet kan starte eller slutte abrupt, da den givne side kan være en del af et større dokument.\n"
        "Hvis der ikke er noget meningsfuldt indhold eller der ikke er givet en side, returner '0', intet andet."
    )
    try:
        result = await image_to_text(file_bytes, prompt)
        return "" if result.strip() == "0" else result
    except Exception as e:
        logger.warning(f"VLM failed for {file_name}: {e}")
        return ""