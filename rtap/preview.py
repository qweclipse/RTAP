"""Bounded, read-only loopback preview; no inference, disk reads or commands per request."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import time
from urllib.parse import urlsplit, parse_qs
import uuid

from rtap.config import ROOT
from rtap.i18n import translator


class PreviewServer:
    def __init__(self, port=8502):
        self.publish(dict(status="starting"))
        token = uuid.uuid4().hex
        owner = self
        documents = {}
        template = (ROOT / "rtap/preview.html").read_text("utf-8")
        for language in ("ru", "ro"):
            t = translator("Română" if language == "ro" else "Русский")
            labels = dict(people=t("people"), off=t("camera_off"), paused=t("camera_paused"), waiting=t("waiting_frame"))
            documents[language] = template.replace("LABELS", json.dumps(labels, ensure_ascii=False)).encode("utf-8")

        class Handler(BaseHTTPRequestHandler):
            def setup(self):
                self.request.settimeout(2)
                super().setup()

            def do_GET(self):
                if self.headers.get("Host") != f"127.0.0.1:{self.server.server_port}":
                    self.send_error(403)
                    return
                parsed = urlsplit(self.path)
                if parsed.path == f"/{token}/frame":
                    body, kind = owner.packet, "application/json"
                elif parsed.path == f"/{token}/":
                    language = parse_qs(parsed.query).get("language", ["ru"])[0]
                    body, kind = documents.get(language, documents["ru"]), "text/html; charset=utf-8"
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", kind)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Content-Security-Policy", "frame-ancestors http://127.0.0.1:8501 http://localhost:8501")
                self.end_headers()
                try:
                    self.wfile.write(body)
                except (OSError, TimeoutError):
                    pass

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", port), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_port}/{token}/"
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs=dict(poll_interval=.1), daemon=True)
        self.thread.start()

    def publish(self, packet):
        self.packet = json.dumps(dict(updated=time.time(), **packet), ensure_ascii=False, allow_nan=False).encode("utf-8")

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=3)
