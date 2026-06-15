from fastapi.responses import RedirectResponse
from starlette.routing import Route
from app.main import app

app.router.routes = [
    route for route in app.router.routes
    if not (
        isinstance(route, Route)
        and getattr(route, "path", None) == "/"
    )
]

@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse("/today/", status_code=307)
