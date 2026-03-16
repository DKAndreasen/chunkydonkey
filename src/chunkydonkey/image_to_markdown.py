import base64
import httpx
import io
import os
from openai import AsyncOpenAI
from pathlib import Path
from PIL import Image
from PIL.ExifTags import TAGS


BASE_URL = os.getenv("VLM_BASE_URL")
API_KEY = os.getenv("VLM_API_KEY")
MODEL = os.getenv("VLM_MODEL")
CONNECT_TIMEOUT = float(os.getenv("VLM_CONNECT_TIMEOUT", "30"))
RESPONSE_TIMEOUT = float(os.getenv("VLM_RESPONSE_TIMEOUT", "300"))
SYSTEM_PROMPT = (Path(__file__).parent / "prompts" / "image_to_chunks.md").read_text().strip()

client = AsyncOpenAI(
    base_url=BASE_URL,
    api_key=API_KEY,
    timeout=httpx.Timeout(RESPONSE_TIMEOUT, connect=CONNECT_TIMEOUT),
)


async def image_to_markdown(file: bytes, ft):

    meta = {'content_type': ft.mime}

    # Extract dimensions + EXIF (lazy, header only)
    try:
        img = Image.open(io.BytesIO(file))
        meta['width'] = img.width
        meta['height'] = img.height
        for tag_id, value in img.getexif().items():
            tag = TAGS.get(tag_id)
            if tag and isinstance(value, (str, int, float)):
                meta[tag] = value
    except Exception:
        pass

    image_b64 = base64.b64encode(file).decode()

    # OCR
    response = await client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": [
                {"type": "image_url", "image_url": {"url": f"data:{ft.mime};base64,{image_b64}"}},
            ]},
        ],
    )

    markdown = response.choices[0].message.content or ""

    return markdown, meta