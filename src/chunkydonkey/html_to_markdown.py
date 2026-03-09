import base64
import hashlib
import html2text
import re
import trafilatura


DATA_URI_RE = re.compile(
    r'''(src\s*=\s*)(["'])data:image/([^;]+);base64,([^"']+)\2''',
    re.IGNORECASE,
)


def html_to_markdown(file: bytes, base_url: str | None = None):

    html = file.decode("utf-8")

    # Extract base64 images
    images = {}
    def replacer(m):
        prefix, quote, img_type, b64_data = m.group(1), m.group(2), m.group(3), m.group(4)
        try:
            img_bytes = base64.b64decode(b64_data)
        except Exception:
            return m.group(0)
        sha256 = hashlib.sha256(img_bytes).hexdigest()
        images[sha256] = img_bytes
        return f"{prefix}{quote}chunkydonkey/{sha256}.jpg{quote}"

    html = DATA_URI_RE.sub(replacer, html)

    # Full HTML document → trafilatura (strips nav, ads, sidebars)
    if re.search(r'<(!doctype|html)\b', re.sub(r'\s+', ' ', html[:10000])[:100], re.IGNORECASE):
        meta = {'content_type': 'text/html'}
        meta_obj = trafilatura.extract_metadata(html, default_url=base_url)
        if meta_obj:
            for field in ("title", "author", "date", "description", "sitename", "license", "pagetype"):
                val = getattr(meta_obj, field, None)
                if val and str(val).strip():
                    meta[field] = str(val).strip()
            for field in ("categories", "tags"):
                val = getattr(meta_obj, field, None)
                if val:
                    meta[field] = list(val)
        markdown = trafilatura.extract(
            html,
            url=base_url,
            output_format="markdown",
            include_images=True,
            include_links=True,
        ) or ""
    
    # Fragment, inline HTML, or plain text → html2text
    else:
        meta = {}
        h = html2text.HTML2Text()
        h.body_width = 0
        markdown = h.handle(html)

    return markdown.encode("utf-8"), images, meta