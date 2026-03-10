import re
from urllib.parse import urljoin


MD_REF_RE = re.compile(r'(!?)\[([^\]]*)\]\(([^)]+)\)')
SEPARATORS = [r'\n(?=#)', r'\n\n', r'\n', r'\. ', r' ']


def markdown_to_chunks(file: bytes, base_url: str | None = None):

    markdown = file.decode("utf-8")

    images = {}
    def replacer(m):
        bang, text, src = m.group(1), m.group(2), m.group(3)
        is_image = bang == "!"
        # Resolve relative URLs
        if not src.startswith(("http://", "https://")) and not re.match(r'^[a-f0-9]{64}$', src):
            if base_url:
                src = urljoin(base_url, src)
            elif is_image:
                return ""
            else:
                return m.group(0)
        # Collect external image URLs for downstream processing
        if is_image and src.startswith(("http://", "https://")):
            images[src] = src
        return f"{bang}[{text}]({src})"

    markdown = MD_REF_RE.sub(replacer, markdown)
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