import os
import traceback
from fastapi import FastAPI, Request
from fastapi.responses import PlainTextResponse

app = FastAPI()
import_error = None

try:
    from app.main import app as real_app
    app = real_app

    @app.get("/__diag")
    def diag():
        return PlainTextResponse(
            "DSM_DIAG_OK\n\n"
            f"DATABASE_URL_exists={bool(os.getenv('DATABASE_URL'))}\n"
            f"PWD={os.getenv('PWD')}\n"
            f"PYTHONPATH={os.getenv('PYTHONPATH')}\n"
        )

    @app.exception_handler(Exception)
    async def debug_exception_handler(request: Request, exc: Exception):
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return PlainTextResponse(
            "DSM_RUNTIME_ERROR\n\n"
            f"path={request.url.path}\n\n"
            + tb,
            status_code=500
        )

except Exception as exc:
    import_error = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

    @app.get("/")
    def diag_root():
        return PlainTextResponse("DSM diagnostic fallback running. Go to /__diag")

    @app.get("/__diag")
    def diag_import():
        return PlainTextResponse(
            "DSM_IMPORT_ERROR\n\n"
            f"DATABASE_URL_exists={bool(os.getenv('DATABASE_URL'))}\n"
            f"PWD={os.getenv('PWD')}\n"
            f"PYTHONPATH={os.getenv('PYTHONPATH')}\n\n"
            + import_error,
            status_code=500
        )

    @app.get("/today")
    def diag_today():
        return diag_import()
