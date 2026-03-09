import hashlib
import re


MD_IMAGE_RE = re.compile(r'!\[([^\]]*)\]\(([^)]+)\)')
SECTION_RE = re.compile(r'\n(?=#)')


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

    return chunks, images


def split_markdown(text: str, target_size: int = 4000) -> list[str]:
    if len(text) <= target_size:
        return [text]

    # Split into sections at headers, then accumulate to target size
    sections = SECTION_RE.split(text)
    chunks = []
    current = ""
    for section in sections:
        if current and len(current) + len(section) > target_size:
            chunks.append(current.strip())
            current = section
        else:
            current = current + section if current else section
    if current.strip():
        chunks.append(current.strip())

    # If any chunk is still too large, split on paragraphs
    result = []
    for chunk in chunks:
        if len(chunk) <= target_size:
            result.append(chunk)
            continue
        paragraphs = chunk.split("\n\n")
        current = ""
        for para in paragraphs:
            if current and len(current) + len(para) + 2 > target_size:
                result.append(current.strip())
                current = para
            else:
                current = current + "\n\n" + para if current else para
        if current.strip():
            result.append(current.strip())

    return result