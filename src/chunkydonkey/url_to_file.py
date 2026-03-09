import asyncio
import hashlib
import ipaddress
import httpx
import os
import re
import socket
from playwright.async_api import async_playwright
from urllib.parse import urlparse


BROWSERLESS_URL = os.getenv("BROWSERLESS_URL", "ws://browserless:3000")
IMG_SRC_RE = re.compile(r'''(src\s*=\s*)(["'])(.+?)\2''', re.IGNORECASE)
MAX_PIXELS = 1024 * 1024
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(100 * 1024 * 1024)))


async def url_to_file(url: str) -> tuple[bytes, dict[str, bytes]]:
    """Fetch URL. Returns (file_bytes, images). Images keyed by sha256.

    Images in the HTML have their src replaced with chunkydonkey/{sha256}.jpg for trafilatura.
    """
    if not await is_safe_url(url):
        raise ValueError(f"URL blocked by SSRF protection: {url}")

    # Try httpx first for known static file extensions
    static_exts = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
                   ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
                   ".ods", ".odp", ".odt", ".csv", ".txt", ".md", ".zip")
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in static_exts):
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "")
            if not ct.startswith("text/html"):
                if len(resp.content) > MAX_FILE_SIZE:
                    raise ValueError(f"Response too large: {len(resp.content)} bytes")
                return resp.content, {}
            # Lied about being a static file — fall through to Playwright

    images: dict[str, bytes] = {}
    url_to_sha256: dict[str, str] = {}

    async with async_playwright() as pw:
        browser = await pw.chromium.connect(BROWSERLESS_URL)
        try:
            page = await browser.new_page()

            async def route_handler(route):
                if route.request.resource_type in ("font", "media"):
                    await route.abort()
                else:
                    await route.continue_()

            await page.route("**/*", route_handler)

            async def on_response(response):
                ct = response.headers.get("content-type", "")
                if ct.startswith("image/") and response.ok:
                    try:
                        body = await response.body()
                        sha256 = hashlib.sha256(body).hexdigest()
                        images[sha256] = body
                        url_to_sha256[response.url] = sha256
                    except Exception:
                        pass

            page.on("response", on_response)

            # Download detection (litecrawl pattern)
            downloads: list = []
            download_event = asyncio.Event()

            def on_download(d):
                downloads.append(d)
                download_event.set()

            page.on("download", on_download)

            try:
                response = await page.goto(url, wait_until="load", timeout=30000)
            except Exception as exc:
                # Navigation can fail when browser triggers a download instead.
                # Wait briefly for the download event before giving up.
                try:
                    await asyncio.wait_for(download_event.wait(), timeout=0.1)
                except TimeoutError:
                    raise exc

                if not downloads:
                    raise exc

                # It was a download — read and return file bytes
                download_path = await downloads[-1].path()
                if not download_path:
                    raise RuntimeError(f"Download save failed for {url}")
                with open(download_path, "rb") as f:
                    file_bytes = f.read()
                if len(file_bytes) > MAX_FILE_SIZE:
                    raise ValueError(f"Download too large: {len(file_bytes)} bytes")
                return file_bytes, images

            # HTML path
            if response is None or not response.ok:
                status = response.status if response else "no response"
                raise RuntimeError(f"Fetch failed for {url}: HTTP {status}")

            # Pre-check content-length header if available
            cl = response.headers.get("content-length", "")
            if cl.isdigit() and int(cl) > MAX_FILE_SIZE:
                raise ValueError(f"Response too large: {cl} bytes")

            # Wait for DOM images to finish loading (bounded)
            try:
                await asyncio.wait_for(
                    page.evaluate("""() => Promise.all(
                        Array.from(document.querySelectorAll('img'))
                            .filter(img => !img.complete)
                            .map(img => new Promise(r => {
                                img.addEventListener('load', r, {once: true});
                                img.addEventListener('error', r, {once: true});
                            }))
                    )"""),
                    timeout=10,
                )
            except Exception:
                pass

            # Map original src attributes (possibly relative) to resolved absolute URLs
            src_to_sha256 = {}
            if url_to_sha256:
                src_map = await page.evaluate("""() =>
                    Object.fromEntries(
                        Array.from(document.querySelectorAll('img[src]'))
                            .map(img => [img.getAttribute('src'), img.src])
                    )
                """)
                for attr_src, resolved_url in src_map.items():
                    sha256 = url_to_sha256.get(resolved_url)
                    if sha256:
                        src_to_sha256[attr_src] = sha256

            html_content = await page.content()
            if src_to_sha256:
                def replacer(m):
                    prefix, quote, src = m.group(1), m.group(2), m.group(3)
                    sha256 = src_to_sha256.get(src)
                    if sha256:
                        return f'{prefix}{quote}chunkydonkey/{sha256}.jpg{quote}'
                    return m.group(0)
                html_content = IMG_SRC_RE.sub(replacer, html_content)
            html_bytes = html_content.encode("utf-8")

            if len(html_bytes) > MAX_FILE_SIZE:
                raise ValueError(f"HTML content too large: {len(html_bytes)} bytes")

            return html_bytes, images

        finally:
            try:
                page.off("download", on_download)
                page.off("response", on_response)
            except Exception:
                pass
            await browser.close()


async def is_safe_url(url: str) -> bool:
    """Prevent SSRF by rejecting URLs that resolve to private/loopback/link-local IPs."""
    try:
        parsed = urlparse(url)
        hostname = parsed.hostname
        if not hostname:
            return False
        scheme = (parsed.scheme or "").lower()
        if scheme not in ("http", "https"):
            return False
        try:
            ip = ipaddress.ip_address(hostname)
            return not (ip.is_private or ip.is_loopback or ip.is_link_local)
        except ValueError:
            pass
        loop = asyncio.get_running_loop()
        try:
            addr_info = await loop.getaddrinfo(hostname, None)
        except socket.gaierror:
            return False
        for _, _, _, _, sockaddr in addr_info:
            ip = ipaddress.ip_address(sockaddr[0])
            if ip.is_private or ip.is_loopback or ip.is_link_local:
                return False
        return True
    except Exception:
        return False