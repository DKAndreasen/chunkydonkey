import os
import re

BARE_URL_RE = re.compile(r'(?<!\]\()(?<!\()(?<!")(https?://[^\s\)\]>"]+)', re.IGNORECASE)
TMP_DIR = os.getenv("TMP_DIR", "/tmp/chunkydonkey")


def linkify_urls(markdown: str) -> str:
    """Convert bare URLs to markdown links: https://x → [https://x](https://x)."""
    return BARE_URL_RE.sub(r'[\1](\1)', markdown)


def normalize_text(file: bytes) -> bytes:
    """Try multiple encodings, normalize EOL, return clean UTF-8 bytes."""
    for encoding in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
        try:
            text = file_bytes.decode(encoding)
            text = text.replace("\r\n", "\n").replace("\r", "\n")
            return text.encode("utf-8")
        except Exception:
            continue
    return file_bytes


def tmp_save(sha256: str, data: bytes) -> str:
    os.makedirs(TMP_DIR, exist_ok=True)
    path = os.path.join(TMP_DIR, sha256)
    with open(path, "wb") as f:
        f.write(data)
    return path


def tmp_load(sha256: str) -> bytes | None:
    path = os.path.join(TMP_DIR, sha256)
    try:
        with open(path, "rb") as f:
            return f.read()
    except FileNotFoundError:
        return None


def tmp_delete(sha256: str):
    try:
        os.remove(os.path.join(TMP_DIR, sha256))
    except FileNotFoundError:
        pass
