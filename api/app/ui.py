from pathlib import Path

from flask import Blueprint, send_from_directory

ui = Blueprint("ui", __name__)
_STATIC = Path(__file__).parent / "static"


@ui.get("/")
def index():
    """Interfaz web (HTML/JS estatico): consume esta misma API desde el navegador."""
    return send_from_directory(_STATIC, "index.html")
