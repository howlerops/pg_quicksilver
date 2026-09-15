"""Freshness, readiness gating and metrics (goals G4 and G7).

docs/08 makes readiness gating non-negotiable in phase 1: "retrofitting a safety
property is how you get an incident first". The mechanism is deliberately dumb —
when lag exceeds `freshnessSLO` the pod fails its readiness probe, Kubernetes
removes it from the Service endpoints, and queries go elsewhere. That is what
turns "bounded staleness" from a claim on a slide into an enforced property.

The failure mode this prevents is the one that matters: a mirror that silently
keeps serving hours-old answers because nothing was watching.

Exposes:
  GET /readyz    200 ready / 503 lagging   <- the Kubernetes readiness probe
  GET /healthz   200 while the process is alive (liveness; never gates on lag)
  GET /metrics   Prometheus text format
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer

from .changestream import lsn_to_int


@dataclass
class Freshness:
    applied_lsn: str = "0/0"
    head_lsn: str = "0/0"
    lag_bytes: int = 0
    lag_seconds: float = 0.0
    #: When the mirror was last known to be CAUGHT UP — not when it last
    #: applied a batch. Those differ on an idle source: no writes means no
    #: applies, and keying off last-apply would make a perfectly fresh mirror
    #: go unready simply because nothing happened. Found by run_phase1.
    caught_up_at: float = field(default_factory=time.time)
    last_apply_at: float = field(default_factory=time.time)
    compaction_backlog: int = 0
    verified_at: float = 0.0
    diverged: bool = False


class MirrorHealth:
    """Thread-safe freshness state plus the readiness decision."""

    def __init__(self, freshness_slo_seconds: float = 5.0):
        self.slo = freshness_slo_seconds
        self._lock = threading.Lock()
        self.f = Freshness()
        self.started = time.time()

    def record_apply(self, applied_lsn: str, head_lsn: str, backlog: int = 0) -> None:
        with self._lock:
            self.f.applied_lsn = applied_lsn
            self.f.head_lsn = head_lsn
            self.f.lag_bytes = max(0, lsn_to_int(head_lsn) - lsn_to_int(applied_lsn))
            self.f.last_apply_at = time.time()
            self.f.compaction_backlog = backlog

    def record_caught_up(self) -> None:
        """The source produced nothing to apply, so the mirror is by definition
        current. Call this on every empty poll — it is what keeps an idle
        database from being reported as stale."""
        with self._lock:
            self.f.caught_up_at = time.time()
            self.f.last_apply_at = time.time()

    def record_verification(self, diverged: bool) -> None:
        with self._lock:
            self.f.verified_at = time.time()
            self.f.diverged = diverged

    def snapshot(self) -> Freshness:
        with self._lock:
            f = Freshness(**self.f.__dict__)
        # Staleness is time since the mirror was last CAUGHT UP. It grows only
        # while there is unapplied work — an idle source keeps it at ~0.
        f.lag_seconds = time.time() - f.caught_up_at
        return f

    def ready(self) -> tuple[bool, str]:
        f = self.snapshot()
        if f.diverged:
            return False, "mirror diverged from source — verification failed"
        if f.lag_seconds > self.slo:
            return False, f"lag {f.lag_seconds:.1f}s exceeds freshnessSLO {self.slo:.1f}s"
        return True, f"lag {f.lag_seconds:.1f}s within SLO {self.slo:.1f}s"

    # ---- exposition ----------------------------------------------------
    def metrics(self) -> str:
        f = self.snapshot()
        ok, _ = self.ready()
        lines = [
            "# HELP quicksilver_lag_seconds Seconds since the last applied batch.",
            "# TYPE quicksilver_lag_seconds gauge",
            f"quicksilver_lag_seconds {f.lag_seconds:.3f}",
            "# HELP quicksilver_lag_bytes WAL bytes between source head and applied_lsn.",
            "# TYPE quicksilver_lag_bytes gauge",
            f"quicksilver_lag_bytes {f.lag_bytes}",
            "# HELP quicksilver_ready 1 if the mirror is inside its freshness SLO.",
            "# TYPE quicksilver_ready gauge",
            f"quicksilver_ready {1 if ok else 0}",
            "# HELP quicksilver_compaction_backlog Delta files awaiting compaction.",
            "# TYPE quicksilver_compaction_backlog gauge",
            f"quicksilver_compaction_backlog {f.compaction_backlog}",
            "# HELP quicksilver_diverged 1 if verification found a mismatch.",
            "# TYPE quicksilver_diverged gauge",
            f"quicksilver_diverged {1 if f.diverged else 0}",
        ]
        return "\n".join(lines) + "\n"

    def serve(self, port: int = 8080) -> HTTPServer:
        health = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):        # quiet
                pass

            def do_GET(self):
                if self.path.startswith("/readyz"):
                    ok, why = health.ready()
                    body = json.dumps({"ready": ok, "reason": why,
                                       **health.snapshot().__dict__}, default=str)
                    self._send(200 if ok else 503, body, "application/json")
                elif self.path.startswith("/healthz"):
                    self._send(200, '{"alive":true}', "application/json")
                elif self.path.startswith("/metrics"):
                    self._send(200, health.metrics(), "text/plain; version=0.0.4")
                else:
                    self._send(404, "not found", "text/plain")

            def _send(self, code, body, ctype):
                data = body.encode()
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        srv = HTTPServer(("127.0.0.1", port), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        return srv
