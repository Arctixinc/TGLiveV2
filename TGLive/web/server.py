from aiohttp import web
from .routes import setup_routes
from .middleware import cors_middleware


def create_app():
    app = web.Application(middlewares=[cors_middleware])
    setup_routes(app)
    return app


async def start_server(port: int = 8000):
    app = create_app()

    runner = web.AppRunner(app)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    return runner

async def stop_server(runner: web.AppRunner):
    if runner is not None:
        await runner.cleanup()