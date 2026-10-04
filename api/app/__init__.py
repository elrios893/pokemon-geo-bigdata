import os

from flask import Flask, jsonify
from pymongo import MongoClient


def create_app(mongo_client=None):
    app = Flask(__name__)
    client = mongo_client or MongoClient(
        os.environ["MONGO_URI"], serverSelectionTimeoutMS=3000
    )
    app.config["DB"] = client[os.environ.get("MONGO_DB", "pokemon")]

    @app.get("/health")
    def health():
        try:
            app.config["DB"].client.admin.command("ping")
        except Exception as exc:  # noqa: BLE001
            return jsonify(status="error", mongo=str(exc)), 503
        return jsonify(status="ok", mongo="up")

    return app
