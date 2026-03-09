import asyncio
import ipaddress
import httpx
import os
import socket
from playwright.async_api import async_playwright
from urllib.parse import urlparse


BROWSERLESS_URL = os.getenv("BROWSERLESS_URL", "ws://browserless:3000")
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(100 * 1024 * 1024)))


async def url_to_file(url: str) -> tuple[bytes, str]:
    """Fetch URL. Returns (file_bytes, resolved_url).

    All relative URLs in HTML are absolutized via page.evaluate.
    Images are blocked — they are handled downstream by the pipeline.
    """
    if not await is_safe_url(url):
        raise ValueError(f"URL blocked by SSRF protection: {url}")

    # Try httpx first for known static file extensions
    static_exts = (".pdf", ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg",
                   ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx",
                   ".ods", ".odp", ".odt", ".csv", ".txt", ".md", ".zip",
                   ".tar", ".gz")
    path = urlparse(url).path.lower()
    if any(path.endswith(ext) for ext in static_exts):
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), follow_redirects=True) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            ct = resp.headers.get("content-type", "")
            if not ct.startswith("text/html"):
                if len(resp.content) > MAX_FILE_SIZE:
                    raise ValueError(f"Response too large: {len(resp.content)} bytes")
                return resp.content, str(resp.url)
            # Lied about being a static file — fall through to Playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.connect(BROWSERLESS_URL)
        try:
            page = await browser.new_page()

            async def route_handler(route):
                if route.request.resource_type in ("font", "media", "image"):
                    await route.abort()
                else:
                    await route.continue_()

            await page.route("**/*", route_handler)

            # Download detection
            downloads: list = []
            download_event = asyncio.Event()

            def on_download(d):
                downloads.append(d)
                download_event.set()

            page.on("download", on_download)

            try:
                response = await page.goto(url, wait_until="load", timeout=30000)
            except Exception as exc:
                try:
                    await asyncio.wait_for(download_event.wait(), timeout=0.1)
                except TimeoutError:
                    raise exc

                if not downloads:
                    raise exc

                download_path = await downloads[-1].path()
                if not download_path:
                    raise RuntimeError(f"Download save failed for {url}")
                with open(download_path, "rb") as f:
                    file_bytes = f.read()
                if len(file_bytes) > MAX_FILE_SIZE:
                    raise ValueError(f"Download too large: {len(file_bytes)} bytes")
                return file_bytes, url

            if response is None or not response.ok:
                status = response.status if response else "no response"
                raise RuntimeError(f"Fetch failed for {url}: HTTP {status}")

            cl = response.headers.get("content-length", "")
            if cl.isdigit() and int(cl) > MAX_FILE_SIZE:
                raise ValueError(f"Response too large: {cl} bytes")

            # Absolutize all href and src attributes
            await page.evaluate("""() => {
                const base = document.baseURI;
                document.querySelectorAll('[href]').forEach(el => {
                    try { el.setAttribute('href', new URL(el.getAttribute('href'), base).href); } catch {}
                });
                document.querySelectorAll('[src]').forEach(el => {
                    try { el.setAttribute('src', new URL(el.getAttribute('src'), base).href); } catch {}
                });
            }""")

            html_bytes = (await page.content()).encode("utf-8")

            if len(html_bytes) > MAX_FILE_SIZE:
                raise ValueError(f"HTML content too large: {len(html_bytes)} bytes")

            return html_bytes, page.url

        finally:
            try:
                page.off("download", on_download)
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
