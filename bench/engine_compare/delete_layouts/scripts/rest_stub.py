#!/usr/bin/env python3
"""Minimal local Iceberg REST catalog stub: one namespace 'default', tables are directories under WH that
already hold metadata (created by DuckDB's COPY ... TO (FORMAT iceberg)). Commits apply add-snapshot /
set-snapshot-ref / set-properties to the raw metadata JSON. Only for this experiment."""
import sys, os, json, glob, uuid, re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WH = sys.argv[1]
PORT = int(sys.argv[2])
LOG = open(os.path.join(WH, "stub.log"), "a")


def cur(t):
    d = os.path.join(WH, "default", t, "metadata")
    hint = os.path.join(d, "current.txt")
    if os.path.exists(hint):
        p = open(hint).read().strip()
    else:
        p = sorted(glob.glob(d + "/*.metadata.json"), key=os.path.getmtime)[-1]
    return p, json.load(open(p))


def apply(md, updates):
    for u in updates:
        a = u["action"]
        if a == "add-snapshot":
            s = u["snapshot"]
            md.setdefault("snapshots", []).append(s)
            md["last-sequence-number"] = max(md.get("last-sequence-number", 0), s.get("sequence-number", 0))
            md["last-updated-ms"] = s["timestamp-ms"]
            if "added-rows" in s and "first-row-id" in s:
                md["next-row-id"] = max(md.get("next-row-id", 0), s["first-row-id"] + s["added-rows"])
        elif a == "set-snapshot-ref":
            r = {k: v for k, v in u.items() if k not in ("action", "ref-name")}
            md.setdefault("refs", {})[u["ref-name"]] = r
            if u["ref-name"] == "main":
                md["current-snapshot-id"] = u["snapshot-id"]
                md.setdefault("snapshot-log", []).append(
                    {"snapshot-id": u["snapshot-id"], "timestamp-ms": md["last-updated-ms"]})
        elif a == "set-properties":
            md.setdefault("properties", {}).update(u["updates"])
        elif a == "remove-properties":
            for k in u["removals"]:
                md.get("properties", {}).pop(k, None)
        elif a in ("remove-snapshots",):
            ids = set(u["snapshot-ids"])
            md["snapshots"] = [s for s in md["snapshots"] if s["snapshot-id"] not in ids]
        elif a in ("upgrade-format-version",):
            md["format-version"] = u["format-version"]
        else:
            raise ValueError("unsupported update " + a)
    return md


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, obj=None):
        b = json.dumps(obj).encode() if obj is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(b)

    def handle_any(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n)) if n else None
        path = self.path.split("?")[0].rstrip("/")
        LOG.write(f"{self.command} {self.path} {json.dumps(body)[:3000] if body else ''}\n"); LOG.flush()
        try:
            if path.endswith("/v1/config"):
                return self.send(200, {"defaults": {}, "overrides": {}})
            if path.endswith("/namespaces"):
                return self.send(200, {"namespaces": [["default"]]})
            if path.endswith("/namespaces/default"):
                return self.send(200, {"namespace": ["default"], "properties": {}})
            if path.endswith("/namespaces/default/tables"):
                ts = [os.path.basename(p) for p in glob.glob(WH + "/default/*") if os.path.isdir(p)]
                return self.send(200, {"identifiers": [{"namespace": ["default"], "name": t} for t in ts]})
            m = re.search(r"/namespaces/default/tables/([^/]+)$", path)
            if m:
                t = m.group(1)
                if not os.path.isdir(os.path.join(WH, "default", t)):
                    return self.send(404, {"error": {"message": "no such table", "type": "NoSuchTableException", "code": 404}})
                p, md = cur(t)
                if self.command in ("GET", "HEAD"):
                    return self.send(200, {"metadata-location": "file://" + p, "metadata": md, "config": {}})
                md = apply(md, body["updates"])
                np = os.path.join(os.path.dirname(p), f"{uuid.uuid4()}.metadata.json")
                json.dump(md, open(np, "w"))
                open(os.path.join(os.path.dirname(p), "current.txt"), "w").write(np)
                return self.send(200, {"metadata-location": "file://" + np, "metadata": md})
            if path.endswith("/transactions/commit"):
                for ch in body["table-changes"]:
                    t = ch["identifier"]["name"]
                    p, md = cur(t)
                    md = apply(md, ch["updates"])
                    np = os.path.join(os.path.dirname(p), f"{uuid.uuid4()}.metadata.json")
                    json.dump(md, open(np, "w"))
                    open(os.path.join(os.path.dirname(p), "current.txt"), "w").write(np)
                return self.send(204)
            return self.send(404, {"error": {"message": "not found " + path, "type": "NotFound", "code": 404}})
        except Exception as e:
            LOG.write(f"ERR {e!r}\n"); LOG.flush()
            return self.send(500, {"error": {"message": repr(e), "type": "Stub", "code": 500}})

    do_GET = do_POST = do_HEAD = do_DELETE = handle_any


ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
