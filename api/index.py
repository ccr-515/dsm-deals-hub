import traceback

try:
    from app.main import app
except Exception:
    print("DSM_DEALS_IMPORT_ERROR_START")
    traceback.print_exc()
    print("DSM_DEALS_IMPORT_ERROR_END")
    raise
