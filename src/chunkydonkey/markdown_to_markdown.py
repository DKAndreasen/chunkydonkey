import re
from urllib.parse import urljoin


MD_REF_RE = re.compile(r'(!?)\[([^\]]*)\]\(([^)]+)\)')


def markdown_to_markdown(file: bytes, base_url: str | None = None):

    markdown = file.decode("utf-8")

    images = set()
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
            images.add(src)
        return f"{bang}[{text}]({src})"

    markdown = MD_REF_RE.sub(replacer, markdown)

    return markdown, images, {'content_type': 'text/markdown'}
