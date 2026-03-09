import base64
import os

import httpx
from openai import AsyncOpenAI

BASE_URL = os.getenv("VLM_BASE_URL", "http://localhost:11434/v1")
API_KEY = os.getenv("VLM_API_KEY", "__CHANGE_ME__")
MODEL = os.getenv("VLM_MODEL", "Qwen/Qwen3-VL-8B-Instruct-FP8")
CONNECT_TIMEOUT = float(os.getenv("VLM_CONNECT_TIMEOUT", "30"))
RESPONSE_TIMEOUT = float(os.getenv("VLM_RESPONSE_TIMEOUT", "300"))

client = AsyncOpenAI(
    base_url=BASE_URL,
    api_key=API_KEY,
    timeout=httpx.Timeout(RESPONSE_TIMEOUT, connect=CONNECT_TIMEOUT),
)

SYSTEM_PDF_PAGE = (
    "You are a document understanding model. Given an image of a document page "
    "and its extracted text layer, produce accurate GitHub Flavored Markdown (GFM).\n\n"
    "Rules:\n"
    "- Use the image as ground truth for layout and visual structure\n"
    "- Use the text layer to get accurate character-level text (avoids OCR errors)\n"
    "- Where text layer is missing or wrong, read from the image\n"
    "- Translate data visualizations (charts, graphs) into markdown tables with a note about the original format\n"
    "- Describe images with alt text: ![descriptive alt text](image)\n"
    "- Use appropriate heading levels (h1-h3) based on visual hierarchy\n"
    "- Preserve list structures, bold, italic as seen in the image\n"
    "- For tables: use GFM table syntax, preserve headers and alignment\n"
    "- Output only markdown, no commentary, no fences"
)


async def ocr_pdf_page(image_bytes: bytes, text_layer: str, mime: str = "image/jpeg") -> str:
    """Send a page image + text layer to VLM for OCR."""
    image_b64 = base64.b64encode(image_bytes).decode()

    context = f"Extracted text layer:\n{text_layer}" if text_layer.strip() else "No text layer (scanned page)."

    response = await client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PDF_PAGE},
            {
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{image_b64}"}},
                    {"type": "text", "text": context},
                ],
            },
        ],
        temperature=0,
        top_p=1.0,
        max_tokens=4096,
    )
    return response.choices[0].message.content or ""
