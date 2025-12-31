import os
import asyncio
import aiohttp
from aiohttp import web
from html import escape



PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

HLS_ROOT = os.path.abspath("hls")




async def status_page(request: web.Request) -> web.Response:
    return web.Response(text="TGLive Streaming Server is Running")




async def handle_hls(request: web.Request) -> web.StreamResponse:
    rel_path = request.match_info.get("path", "").lstrip("/")

    # 🔒 Prevent directory traversal
    if ".." in rel_path:
        return web.Response(status=400, text="Invalid path")

    abs_path = os.path.abspath(os.path.join(HLS_ROOT, rel_path))

    if not abs_path.startswith(HLS_ROOT):
        return web.Response(status=403, text="Access denied")

    if not os.path.exists(abs_path):
        return web.Response(status=404, text="File not found")

    if abs_path.endswith(".m3u8"):
        return web.FileResponse(
            abs_path,
            headers={"Content-Type": "application/x-mpegURL"},
        )

    if abs_path.endswith(".ts"):
        return web.FileResponse(
            abs_path,
            headers={"Content-Type": "video/mp2t"},
        )

    return web.FileResponse(abs_path)



async def file_browser(request: web.Request) -> web.Response:
    rel_path = request.query.get("path", "").lstrip("/")
    view_mode = request.query.get("view") == "1"

    abs_path = os.path.abspath(os.path.join(PROJECT_ROOT, rel_path))

    if not abs_path.startswith(PROJECT_ROOT):
        return web.Response(status=403, text="Access denied")

    if os.path.isfile(abs_path):
        ext = os.path.splitext(abs_path)[1].lower()
        viewable_exts = {
            ".sh", ".py", ".txt", ".env", ".log", ".json", ".yml", ".yaml"
        }

        if ext in viewable_exts or view_mode:
            try:
                with open(abs_path, "r", errors="ignore") as f:
                    content = f.read()
            except Exception:
                return web.Response(status=500, text="Unable to read file")

            return web.Response(
                text=f"<pre>{escape(content)}</pre>",
                content_type="text/html",
                charset="utf-8",
                headers={"Content-Disposition": "inline"},
            )

        return web.FileResponse(
            abs_path,
            headers={
                "Content-Disposition": f'attachment; filename="{os.path.basename(abs_path)}"'
            },
        )

    if os.path.isdir(abs_path):
        items = sorted(os.listdir(abs_path))

        html = "<h2>File Explorer</h2>"
        html += f"<p>Current: /{escape(rel_path)}</p><ul>"

        if rel_path:
            parent = os.path.dirname(rel_path.rstrip("/"))
            html += f'<li><a href="/explorer?path={parent}">..</a></li>'

        for item in items:
            item_rel = os.path.join(rel_path, item)
            item_abs = os.path.join(abs_path, item)
            icon = "📁" if os.path.isdir(item_abs) else "📄"

            if os.path.isfile(item_abs):
                link = f"/explorer?path={item_rel}&view=1"
            else:
                link = f"/explorer?path={item_rel}"

            html += (
                f'<li>{icon} '
                f'<a href="{link}">{escape(item)}</a>'
                f'</li>'
            )

        html += "</ul>"
        return web.Response(text=html, content_type="text/html")

    return web.Response(status=404, text="Not found")


async def stream_logs(request: web.Request) -> web.StreamResponse:
    log_file = "log.txt"

    response = web.StreamResponse(
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
        }
    )

    await response.prepare(request)

    try:
        f = open(log_file, "r", errors="ignore")
        f.seek(0)
    except FileNotFoundError:
        await response.write(b"data: log.txt not found\n\n")
        await response.write_eof()
        return response

    try:
        while True:
            line = f.readline()
            if line:
                try:
                    await response.write(line.encode("utf-8"))
                except (
                    ConnectionResetError,
                    aiohttp.ClientConnectionResetError,
                ):
                    break
            else:
                await asyncio.sleep(0.3)
    finally:
        f.close()

    return response
