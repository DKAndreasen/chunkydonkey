# src/fatingest/vlm.py

import base64
import os
import httpx
from openai import OpenAI


BASE_URL = os.getenv("VLM_API_URL", "http://localhost:11434/v1")
API_KEY = os.getenv("VLM_API_KEY", "__CHANGE_ME__")
MODEL = os.getenv("VLM_MODEL", "Qwen/Qwen3-VL-8B-Instruct-FP8")
CONNECT_TIMEOUT = float(os.getenv("VLM_CONNECT_TIMEOUT", "30"))
RESPONSE_TIMEOUT = float(os.getenv("VLM_RESPONSE_TIMEOUT", "300"))


client = OpenAI(
    base_url=BASE_URL,
    api_key=API_KEY,
    timeout=httpx.Timeout(RESPONSE_TIMEOUT, connect=CONNECT_TIMEOUT),
)


async def image_to_text(image_bytes: bytes, prompt: str) -> str:
    """
    Sends image bytes and prompt to VLM, returns response text.
    """
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                ],
            }
        ],
        temperature=0,
        top_p=1.0,
        max_tokens=4096,
        #extra_body={"repetition_penalty": 1.1},
        extra_body={
            #"repetition_penalty": 1.1,
            "top_k": 1,
            "mm_processor_kwargs": {
                "max_pixels": 4096 * 32 * 32,  # 4,194,304  -> tillad ~2048x2048
                "min_pixels": 256 * 32 * 32,   # valgfrit
            },
        },
    )
    return response.choices[0].message.content or ""