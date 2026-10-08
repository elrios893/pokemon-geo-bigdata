import os

from flask import Flask, jsonify


def create_app(db=None):
    """Fabrica de la app. `db` permite inyectar una base falsa en las pruebas (sin MongoDB)."""
    app = Flask(__name__)
    app.json.sort_keys = False

    if db is None:
        from pymongo import MongoClient  # import perezoso: las pruebas no lo necesitan
        client = MongoClient(os.environ["MONGO_URI"], serverSelectionTimeoutMS=3000)
        db = client[os.environ.get("MONGO_DB", "pokemon")]
    app.config["DB"] = db

    # Indices de las agregaciones de Spark que consulta el mapa (idempotente; si la base no los admite, se sigue sin ellos).
    try:
        db.agg_grid.create_index([("count", -1)])
        db.agg_grid.create_index([("lat_c", 1), ("lng_c", 1)])
    except Exception:  # noqa: BLE001
        app.logger.warning("no se pudieron crear los indices de agg_grid")

    from .routes import bp
    app.register_blueprint(bp)

    from .ui import ui
    app.register_blueprint(ui)

    @app.get("/health")
    def health():
        try:
            app.config["DB"].client.admin.command("ping")
        except Exception as exc:  # noqa: BLE001
            return jsonify(status="error", mongo=str(exc)), 503
        return jsonify(status="ok", mongo="up")

    @app.errorhandler(404)
    def not_found(_):
        return jsonify(error="ruta no encontrada"), 404

    @app.errorhandler(405)
    def method_not_allowed(_):
        return jsonify(error="metodo no permitido"), 405

    @app.errorhandler(Exception)
    def unexpected(exc):
        # Errores de Mongo (p. ej. poligono con autointersecciones, tiempo agotado) -> respuesta limpia.
        from werkzeug.exceptions import HTTPException
        if isinstance(exc, HTTPException):
            return jsonify(error=exc.description), exc.code
        app.logger.exception("error no controlado")
        name = type(exc).__name__
        if name in ("OperationFailure", "ExecutionTimeout"):
            return jsonify(error="la consulta fue rechazada por la base de datos", detail=str(exc)[:200]), 400
        return jsonify(error="error interno"), 500

    return app
