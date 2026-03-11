import re

BARE_URL_RE = re.compile(r'(?<!\]\()(?<!\()(?<!")(https?://[^\s\)\]>"]+)', re.IGNORECASE)
MD_URL_RE = re.compile(r'\[([^\]]*)\]\(([^)]+)\)')
SEPARATORS = [r'\n(?=#)', r'\n\n', r'\n', r'\. ', r' ']


def linkify_urls(markdown: str) -> str:
    """Convert bare URLs to markdown links: https://x → [https://x](https://x)."""
    return BARE_URL_RE.sub(r'[\1](\1)', markdown)


def extract_urls(text: str) -> list[str]:
    """Extract URLs from markdown links: [text](url) → [url, ...]."""
    return [url for _, url in MD_URL_RE.findall(text)]


def normalize_text(file: bytes) -> bytes:
    """Try multiple encodings, normalize EOL, return clean UTF-8 bytes."""
    for encoding in ("utf-8", "latin-1", "cp1252", "iso-8859-1"):
        try:
            text = file.decode(encoding)
            text = text.replace("\r\n", "\n").replace("\r", "\n")
            return text.encode("utf-8")
        except Exception:
            continue
    return file


def split_markdown(text: str, target_size: int = 4000) -> list[str]:
    if len(text) <= target_size:
        return [text]

    # Phase 1: split oversized chunks with increasingly fine separators
    chunks = [text]
    for sep in SEPARATORS:
        result = []
        for chunk in chunks:
            if len(chunk) <= target_size:
                result.append(chunk)
                continue
            parts = re.split(f'({sep})', chunk)
            for part in parts:
                result.append(part)
        chunks = result

    # Phase 2: merge small chunks up to target size
    merged = []
    current = ""
    for chunk in chunks:
        if current and len(current) + len(chunk) > target_size:
            merged.append(current.strip())
            current = chunk
        else:
            current = current + chunk if current else chunk
    if current.strip():
        merged.append(current.strip())

    return merged