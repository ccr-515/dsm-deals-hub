import traceback
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

try:
    from app.main import app as real_app
    app = real_app
except Exception:
    import_error = traceback.format_exc()

    app = FastAPI()

    @app.get("/")
    def diag_root():
        return PlainTextResponse("DSM Deals diagnostic app is running. Go to /__diag")

    @app.get("/today")
    def diag_today():
        return PlainTextResponse(
            "DSM_DEALS_IMPORT_ERROR\n\n" + import_error,
            status_code=500
        )

    @app.get("/__diag")
    def diag():
        return PlainTextResponse(
            "DSM_DEALS_IMPORT_ERROR\n\n" + import_error,
            status_code=500
        )
