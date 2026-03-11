import hashlib
import pymupdf
import re
from collections import Counter

TABLE_LINE_RE = re.compile(r'^\|.+\|$')
SEP_LINE_RE = re.compile(r'^\|[-| :]+\|$')


def pdf_to_markdown(file: bytes):
    images = set()

    with pymupdf.open(stream=file) as doc:
        # Pass 1: cache page dicts, detect body size + header levels
        size_counter = Counter()
        header_sizes = set()
        page_dicts = []

        for page in doc:
            pd = page.get_text("dict")
            page_dicts.append(pd)
            for block in pd["blocks"]:
                if block["type"] != 0:
                    continue
                spans = [s for l in block["lines"] for s in l["spans"] if s["text"].strip()]
                if not spans:
                    continue
                size = round(spans[0]["size"], 1)
                size_counter[size] += len(spans)

        if not size_counter:
            body_size = 0
            size_to_level = {}
        else:
            body_size = size_counter.most_common(1)[0][0]

            for pd in page_dicts:
                for block in pd["blocks"]:
                    if block["type"] != 0:
                        continue
                    spans = [s for l in block["lines"] for s in l["spans"] if s["text"].strip()]
                    if not spans:
                        continue
                    size = round(spans[0]["size"], 1)
                    bold = all(s["flags"] & 2**4 for s in spans)
                    if size > body_size:
                        header_sizes.add(size)
                    elif bold and len(block["lines"]) <= 2:
                        header_sizes.add(body_size + 0.01)

            size_to_level = {s: i + 1 for i, s in enumerate(sorted(header_sizes, reverse=True)[:3])}

        # Pass 2: extract pages
        pages = []
        for page, page_dict in zip(doc, page_dicts):
            links = page.get_links()
            image_info = page.get_image_info(xrefs=True)

            def find_link(cx, cy, _links=links):
                for link in _links:
                    if pymupdf.Rect(link["from"]).contains(pymupdf.Point(cx, cy)):
                        return link.get("uri")

            def find_xref(block_rect, _info=image_info):
                for img in _info:
                    cx = (img["bbox"][0] + img["bbox"][2]) / 2
                    cy = (img["bbox"][1] + img["bbox"][3]) / 2
                    if block_rect.contains(pymupdf.Point(cx, cy)):
                        return img["xref"]

            # Find tables on the page
            tables = page.find_tables(strategy="lines_strict")
            tab_rects = []
            tab_markdowns = []
            for t in tables.tables:
                if t.row_count < 2 or t.col_count < 2:
                    continue
                rect = pymupdf.Rect(t.bbox)
                if t.header.bbox:
                    rect |= pymupdf.Rect(t.header.bbox)
                tab_rects.append(rect)
                tab_markdowns.append(t.to_markdown(clean=False))

            def block_in_table(bbox):
                """Check if a block overlaps any table rect."""
                br = pymupdf.Rect(bbox)
                for i, tr in enumerate(tab_rects):
                    if abs(br & tr) > 0:
                        return i
                return None

            blocks = page_dict["blocks"]
            blocks.sort(key=lambda b: (b["bbox"][1], b["bbox"][0]))

            # Merge continuations
            merged = []
            for block in blocks:
                if block["type"] != 0:
                    merged.append(block)
                    continue
                first = ""
                for l in block["lines"]:
                    for s in l["spans"]:
                        first = s["text"].strip()
                        if first:
                            break
                    if first:
                        break
                if (merged and merged[-1]["type"] == 0 and first
                        and block["bbox"][1] - merged[-1]["bbox"][3] < 5):
                    merged[-1]["lines"].extend(block["lines"])
                    merged[-1]["bbox"] = (
                        min(merged[-1]["bbox"][0], block["bbox"][0]),
                        merged[-1]["bbox"][1],
                        max(merged[-1]["bbox"][2], block["bbox"][2]),
                        block["bbox"][3],
                    )
                else:
                    merged.append(block)

            output = []
            written_tables = set()
            for block in merged:
                if block["type"] == 1:
                    xref = find_xref(pymupdf.Rect(block["bbox"]))
                    if xref:
                        img_data = doc.extract_image(xref)
                        sha256 = hashlib.sha256(img_data["image"]).hexdigest()
                        images.add(img_data["image"])
                        output.append((block["bbox"], f"![]({sha256})"))
                    continue

                # Check if this text block is inside a table
                tab_idx = block_in_table(block["bbox"])
                if tab_idx is not None:
                    if tab_idx not in written_tables:
                        written_tables.add(tab_idx)
                        output.append((tuple(tab_rects[tab_idx]), tab_markdowns[tab_idx].strip()))
                    continue

                spans = [s for l in block["lines"] for s in l["spans"] if s["text"].strip()]
                if not spans:
                    continue
                size = round(spans[0]["size"], 1)
                bold = all(s["flags"] & 2**4 for s in spans)

                level = size_to_level.get(size)
                if not level and bold and len(block["lines"]) <= 2:
                    level = size_to_level.get(body_size + 0.01)

                block_lines = []
                for line in block["lines"]:
                    parts = []
                    for span in line["spans"]:
                        text = span["text"].strip()
                        if not text:
                            continue
                        if span["flags"] & 2**4:
                            text = f"**{text}**"
                        if span["flags"] & 2**1:
                            text = f"*{text}*"
                        cx = (span["bbox"][0] + span["bbox"][2]) / 2
                        cy = (span["bbox"][1] + span["bbox"][3]) / 2
                        uri = find_link(cx, cy)
                        if uri:
                            text = f"[{text}]({uri})"
                        parts.append(text)
                    if parts:
                        block_lines.append(" ".join(parts))

                if block_lines:
                    text_out = "\n".join(block_lines)
                    if level:
                        text_out = f"{'#' * level} {text_out}"
                    output.append((block["bbox"], text_out))

            # Join
            parts = []
            for i, (bbox, text) in enumerate(output):
                if i == 0:
                    parts.append(text)
                    continue
                prev_bbox = output[i - 1][0]
                first_char = text.lstrip()[:1]
                if abs(prev_bbox[0] - bbox[0]) < 3 and first_char and re.match(r'\w', first_char):
                    parts[-1] += " " + text
                else:
                    parts.append(text)

            pages.append("\n\n".join(parts))

        # Fix tables continuing across pages
        pages = merge_cross_page_tables(pages)

        # Merge pages with separator comment
        markdown = ("\n".join(
            f"<!--page:{i+1}-->\n{page.strip()}"
            for i, page in enumerate(pages)
        )).strip()

        meta = {'content_type': 'application/pdf'}
        meta |= doc.metadata or {}

    return markdown, images, meta


def merge_cross_page_tables(pages):
    """Normalize cross-page table continuations with correct headers."""
    for i in range(len(pages) - 1):
        a = pages[i].rstrip().split('\n')
        b = pages[i + 1].lstrip().split('\n')
        if not TABLE_LINE_RE.match(a[-1].strip()):
            continue
        if len(b) < 3 or not TABLE_LINE_RE.match(b[0].strip()) or not SEP_LINE_RE.match(b[1].strip()):
            continue
        # Find page i's table header (line before separator)
        header = None
        for k in range(len(a) - 1, -1, -1):
            if SEP_LINE_RE.match(a[k].strip()) and k > 0:
                header = a[k - 1]
                break
            if not TABLE_LINE_RE.match(a[k].strip()):
                break
        if not header or header.count('|') != b[0].count('|'):
            continue
        if header.strip() == b[0].strip():
            continue  # header already repeated correctly
        # Replace fake header with real one, demote fake to data row
        fake = b[0]
        b[0] = header
        b.insert(2, fake)
        pages[i + 1] = '\n'.join(b)
    return pages