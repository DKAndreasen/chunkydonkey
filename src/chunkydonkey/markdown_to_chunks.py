import hashlib
import re


MD_IMAGE_RE = re.compile(r'!\[([^\]]*)\]\(([^)]+)\)')
SEPARATORS = [r'\n(?=#)', r'\n\n', r'\n', r'\. ', r' ']


def markdown_to_chunks(file: bytes):

    markdown = file.decode("utf-8")

    # Extract and normalize images
    images = {}
    def replacer(m):
        alt, src = m.group(1), m.group(2)
        # Already a chunkydonkey placeholder → keep
        if src.startswith("chunkydonkey/"):
            return m.group(0)
        # Absolute URL → hash URL, store, replace
        if src.startswith(("http://", "https://")):
            sha256 = hashlib.sha256(src.encode("utf-8")).hexdigest()
            images[sha256] = src
            return f"![{alt}](chunkydonkey/{sha256}.jpg)"
        # Relative URL → remove
        return ""

    markdown = MD_IMAGE_RE.sub(replacer, markdown)
    chunks = split_markdown(markdown)

    return chunks, images, {'content_type': 'text/markdown'}


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