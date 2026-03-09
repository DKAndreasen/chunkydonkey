import httpx
import io
import os
import xml.etree.ElementTree as ET
import zipfile


GOTENBERG_URL = os.getenv("GOTENBERG_URL", "http://gotenberg:3000")


async def office_to_pdf(file: bytes, ft) -> tuple[bytes, dict]:
    endpoint = f"{GOTENBERG_URL}/forms/libreoffice/convert"
    form_name = f"document.{ft.extension}"
    meta = office_meta(file)
    file = inject_table_borders(file, ft.extension)

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=10.0)) as client:
        response = await client.post(
            endpoint,
            files={"files": (form_name, file, ft.mime)},
        )
        response.raise_for_status()
        return response.content, meta


def inject_table_borders(file: bytes, extension: str) -> bytes:
    """Inject thin borders on tables in docx/pptx so they render as vectors in PDF."""
    if extension not in ("docx", "pptx"):
        return file

    w = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
    a = "http://schemas.openxmlformats.org/drawingml/2006/main"
    ET.register_namespace("w", w)
    ET.register_namespace("a", a)

    buf = io.BytesIO(file)
    out = io.BytesIO()
    with zipfile.ZipFile(buf, "r") as zin, zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            data = zin.read(item.filename)

            if extension == "docx" and item.filename == "word/document.xml":
                tree = ET.parse(io.BytesIO(data))
                for tbl in tree.iter(f"{{{w}}}tbl"):
                    tblPr = tbl.find(f"{{{w}}}tblPr")
                    if tblPr is None:
                        tblPr = ET.SubElement(tbl, f"{{{w}}}tblPr")
                        tbl.insert(0, tblPr)
                    old = tblPr.find(f"{{{w}}}tblBorders")
                    if old is not None:
                        tblPr.remove(old)
                    borders = ET.SubElement(tblPr, f"{{{w}}}tblBorders")
                    for side in ("top", "left", "bottom", "right", "insideH", "insideV"):
                        ET.SubElement(borders, f"{{{w}}}{side}",
                                      {"w:val": "single", "w:sz": "4", "w:space": "0", "w:color": "000000"})
                xml_buf = io.BytesIO()
                tree.write(xml_buf, xml_declaration=True, encoding="UTF-8")
                data = xml_buf.getvalue()

            elif extension == "pptx" and item.filename.startswith("ppt/slides/slide") and item.filename.endswith(".xml"):
                tree = ET.parse(io.BytesIO(data))
                for tc in tree.iter(f"{{{a}}}tc"):
                    tcPr = tc.find(f"{{{a}}}tcPr")
                    if tcPr is None:
                        tcPr = ET.SubElement(tc, f"{{{a}}}tcPr")
                        tc.insert(0, tcPr)
                    for side in ("lnL", "lnR", "lnT", "lnB"):
                        old = tcPr.find(f"{{{a}}}{side}")
                        if old is not None:
                            tcPr.remove(old)
                        ln = ET.SubElement(tcPr, f"{{{a}}}{side}",
                                           {"w": "6350", "cap": "flat", "cmpd": "sng", "algn": "ctr"})
                        fill = ET.SubElement(ln, f"{{{a}}}solidFill")
                        ET.SubElement(fill, f"{{{a}}}srgbClr", {"val": "000000"})
                xml_buf = io.BytesIO()
                tree.write(xml_buf, xml_declaration=True, encoding="UTF-8")
                data = xml_buf.getvalue()

            zout.writestr(item, data)
    return out.getvalue()


def office_meta(file: bytes) -> dict:
    """Extract metadata from modern office formats (.docx, .xlsx, .pptx) via ZIP stdlib."""
    meta = {}
    try:
        with zipfile.ZipFile(io.BytesIO(file)) as zf:
            # Dublin Core metadata in docProps/core.xml
            if "docProps/core.xml" in zf.namelist():
                tree = ET.parse(zf.open("docProps/core.xml"))
                root = tree.getroot()
                ns = {
                    "dc": "http://purl.org/dc/elements/1.1/",
                    "dcterms": "http://purl.org/dc/terms/",
                    "cp": "http://schemas.openxmlformats.org/package/2006/metadata/core-properties",
                }
                for tag, key in [
                    ("dc:title", "title"),
                    ("dc:creator", "author"),
                    ("dc:subject", "subject"),
                    ("dc:description", "description"),
                    ("cp:keywords", "keywords"),
                    ("cp:category", "category"),
                    ("cp:lastModifiedBy", "last_modified_by"),
                    ("dcterms:created", "created"),
                    ("dcterms:modified", "modified"),
                ]:
                    elem = root.find(tag, ns)
                    if elem is not None and elem.text and elem.text.strip():
                        meta[key] = elem.text.strip()
            # Application info in docProps/app.xml
            if "docProps/app.xml" in zf.namelist():
                tree = ET.parse(zf.open("docProps/app.xml"))
                root = tree.getroot()
                ns_app = {"ep": "http://schemas.openxmlformats.org/officeDocument/2006/extended-properties"}
                for tag, key in [
                    ("ep:Application", "application"),
                    ("ep:Company", "company"),
                ]:
                    elem = root.find(tag, ns_app)
                    if elem is not None and elem.text and elem.text.strip():
                        meta[key] = elem.text.strip()
    except (zipfile.BadZipFile, ET.ParseError, KeyError):
        pass
    return meta