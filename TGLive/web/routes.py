from aiohttp import web
from .views import (
    status_page,
    handle_hls,
    file_browser,
    stream_logs,
)
from .playlist_route import playlist_handler

def setup_routes(app: web.Application):
    app.router.add_get("/", status_page)
    app.router.add_get("/hls/{path:.*}", handle_hls)
    app.router.add_get("/explorer", file_browser)
    app.router.add_get("/live-logs", stream_logs)
    app.router.add_get("/playlist.m3u", playlist_handler)
