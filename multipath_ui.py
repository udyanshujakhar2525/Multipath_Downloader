#!/usr/bin/env python3
"""
multipath_ui.py - a local web UI for multipath.py.

Starts a small server on 127.0.0.1 and opens the page in your browser. The
download engine is multipath.py, which must sit next to this file.

    pip install requests psutil
    python multipath_ui.py

Options:
    --port 8765        pick a port
    --no-browser       don't open a browser window
    --folder PATH      where to save files (default: your Downloads folder)
"""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import socket
import sys
import threading
import time
import uuid
import webbrowser
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

try:
    import multipath as mp
except ImportError:
    sys.exit("multipath.py must sit in the same folder as this file.")

# Colours are assigned to networks in discovery order and reused everywhere in
# the UI, so a network keeps one identity across the graph, rail and file bars.
TRACE_COLOURS = ["#4CC2FF", "#6BE4A8", "#FFB860", "#C89BFF", "#FF7A8A", "#5BD6D6"]

SAMPLE_INTERVAL = 0.5
HISTORY_SAMPLES = 150  # ~75 seconds of graph
CONFIG_PATH = Path.home() / ".multipath-ui.json"


def default_folder() -> Path:
    candidate = Path.home() / "Downloads"
    return candidate if candidate.is_dir() else Path.home()


class Quiet(mp.Download):
    """The engine's monitor thread, minus the terminal rendering."""

    def render(self, fancy: bool, first: bool) -> int:
        return 0


class Job:
    def __init__(self, url: str, folder: Path):
        self.id = uuid.uuid4().hex[:12]
        self.url = url
        self.folder = folder
        self.name = Path(unquote(urlparse(url).path)).name or "download"
        self.status = "queued"  # queued checking downloading paused done error
        self.total = 0
        self.downloaded = 0
        self.rate = 0.0
        self.error = ""
        self.added = time.time()
        self.finished_at: float | None = None
        self.target: Path | None = None
        self.shares: dict[str, int] = {}
        # Carried across restarts: a new Download counts only the bytes it
        # fetches, so without this the per-network split resets on every
        # pause, resume or network toggle.
        self.share_base: dict[str, int] = {}
        self.download: Quiet | None = None
        self.state: mp.State | None = None
        self.remote: mp.RemoteFile | None = None
        self.pausing = False
        self.requeue = False
        self.thread: threading.Thread | None = None

    def as_dict(self) -> dict:
        eta = None
        if self.status == "downloading" and self.rate > 1 and self.total:
            eta = (self.total - self.downloaded) / self.rate
        return {
            "id": self.id,
            "url": self.url,
            "name": self.name,
            "status": self.status,
            "total": self.total,
            "downloaded": self.downloaded,
            "rate": self.rate,
            "eta": eta,
            "error": self.error,
            "shares": self.shares,
            "path": str(self.target) if self.target else None,
        }


class Manager:
    def __init__(self, folder: Path, connections: int):
        self.lock = threading.RLock()
        self.jobs: dict[str, Job] = {}
        self.order: list[str] = []
        self.folder = folder
        self.connections = connections
        self.networks: dict[str, dict] = {}
        self.history: deque[dict] = deque(maxlen=HISTORY_SAMPLES)
        self.load_config()
        self.refresh_networks()
        threading.Thread(target=self._sampler, daemon=True).start()
        threading.Thread(target=self._scheduler, daemon=True).start()

    # -- config ----------------------------------------------------------
    def load_config(self) -> None:
        try:
            data = json.loads(CONFIG_PATH.read_text("utf-8"))
        except Exception:
            return
        saved = data.get("networks", {})
        if isinstance(saved, dict):
            self.networks = {
                str(k): {"enabled": bool(v.get("enabled", True)),
                         "limit": v.get("limit") or None}
                for k, v in saved.items() if isinstance(v, dict)
            }
        folder = data.get("folder")
        if folder and Path(folder).is_dir():
            self.folder = Path(folder)
        if isinstance(data.get("connections"), int):
            self.connections = max(1, min(16, data["connections"]))

    def save_config(self) -> None:
        payload = {
            "folder": str(self.folder),
            "connections": self.connections,
            "networks": {
                name: {"enabled": cfg["enabled"], "limit": cfg["limit"]}
                for name, cfg in self.networks.items()
            },
        }
        try:
            CONFIG_PATH.write_text(json.dumps(payload, indent=2), "utf-8")
        except Exception:
            pass

    # -- networks --------------------------------------------------------
    def refresh_networks(self) -> None:
        found = mp.discover_adapters()
        with self.lock:
            live = {a.name: a.ip for a in found}
            for index, adapter in enumerate(found):
                entry = self.networks.setdefault(
                    adapter.name, {"enabled": True, "limit": None}
                )
                entry["ip"] = adapter.ip
                entry["colour"] = TRACE_COLOURS[index % len(TRACE_COLOURS)]
            for name, entry in self.networks.items():
                entry["present"] = name in live
                entry.setdefault("colour", TRACE_COLOURS[0])
                entry.setdefault("ip", "")

    def enabled_adapters(self) -> list[mp.Adapter]:
        with self.lock:
            out = []
            for name, cfg in self.networks.items():
                if cfg.get("present") and cfg.get("enabled"):
                    out.append(mp.Adapter(
                        name=name, ip=cfg["ip"],
                        limiter=mp.RateLimiter(cfg.get("limit")),
                    ))
            return out

    def set_network(self, name: str, enabled=None, limit=...) -> None:
        with self.lock:
            cfg = self.networks.get(name)
            if not cfg:
                return
            switched = enabled is not None and bool(enabled) != cfg["enabled"]
            if enabled is not None:
                cfg["enabled"] = bool(enabled)
            if limit is not ...:
                cfg["limit"] = limit

            for job in self.jobs.values():
                if job.status != "downloading" or not job.download:
                    continue
                # A limit can be re-applied to live connections in place.
                if limit is not ...:
                    for adapter in job.download.adapters:
                        if adapter.name == name:
                            adapter.limiter = mp.RateLimiter(limit)
                # Switching a link on or off cannot be: the running download
                # holds a fixed set of adapters. Restart it so the switch means
                # what it says - finished chunks are kept, so this costs little.
                if switched:
                    job.requeue = True
                    job.pausing = True
                    job.download.stop.set()
        self.save_config()

    # -- jobs ------------------------------------------------------------
    def add(self, url: str) -> Job:
        job = Job(url, self.folder)
        with self.lock:
            self.jobs[job.id] = job
            self.order.append(job.id)
        return job

    def get(self, job_id: str) -> Job | None:
        with self.lock:
            return self.jobs.get(job_id)

    def pause(self, job_id: str) -> None:
        job = self.get(job_id)
        if not job:
            return
        with self.lock:
            if job.status == "queued":
                job.status = "paused"
                return
            if job.status != "downloading" or not job.download:
                return
            job.pausing = True
            job.download.stop.set()

    def resume(self, job_id: str) -> None:
        job = self.get(job_id)
        with self.lock:
            if job and job.status in ("paused", "error"):
                job.error = ""
                job.status = "queued"

    def remove(self, job_id: str, delete_file: bool) -> None:
        job = self.get(job_id)
        if not job:
            return
        self.pause(job_id)
        for _ in range(40):  # let the worker threads unwind
            if job.status != "downloading":
                break
            time.sleep(0.1)
        with self.lock:
            self.jobs.pop(job_id, None)
            if job_id in self.order:
                self.order.remove(job_id)
        if delete_file and job.target:
            for path in (job.target, Path(str(job.target) + ".mpstate")):
                try:
                    path.unlink()
                except OSError:
                    pass

    # -- the loop that actually runs downloads ---------------------------
    def _scheduler(self) -> None:
        while True:
            time.sleep(0.25)
            with self.lock:
                busy = any(j.status in ("checking", "downloading")
                           for j in self.jobs.values())
                if busy:
                    continue
                nxt = next((self.jobs[i] for i in self.order
                            if self.jobs.get(i) and self.jobs[i].status == "queued"), None)
                if not nxt:
                    continue
                nxt.status = "checking"
                nxt.thread = threading.Thread(target=self._run, args=(nxt,), daemon=True)
                nxt.thread.start()

    def _run(self, job: Job) -> None:
        try:
            adapters = self.enabled_adapters()
            if not adapters:
                raise RuntimeError("No networks are switched on.")

            if job.remote is None:
                job.remote = mp.probe_remote(mp.make_session(None), job.url)
                job.name = job.remote.filename
                job.total = job.remote.size or 0

            usable = mp.usable_adapters(adapters, job.remote.url)
            if not usable:
                raise RuntimeError("No switched-on network could reach that link.")

            job.folder.mkdir(parents=True, exist_ok=True)
            job.target = job.folder / job.remote.filename

            if not job.remote.supports_ranges or not job.remote.size:
                job.status = "downloading"
                self._single(job, usable[0])
                return

            chunk = max(mp.MIN_CHUNK, min(mp.MAX_CHUNK,
                                          job.remote.size // mp.TARGET_CHUNKS or mp.MIN_CHUNK))
            state = mp.State(Path(str(job.target) + ".mpstate"))
            if not (state.load(job.remote.url, job.remote.size, chunk, job.remote.etag)
                    and job.target.exists()):
                state.done.clear()
                state.start(job.remote.url, job.remote.size, chunk, job.remote.etag)
                with open(job.target, "wb") as handle:
                    handle.truncate(job.remote.size)
            job.state = state

            job.share_base = dict(job.shares)
            download = Quiet(job.remote, usable, job.target,
                             self.connections, chunk, state)
            job.download = download
            job.status = "downloading"
            completed = download.run(fancy=False)

            # Start from what earlier runs contributed, so a network that is
            # switched off keeps credit for the bytes it already pulled.
            shares = dict(job.share_base)
            for adapter in usable:
                shares[adapter.name] = job.share_base.get(adapter.name, 0) + adapter.downloaded
            job.shares = shares
            job.downloaded = download.downloaded
            job.rate = 0.0

            if completed:
                state.clear()
                job.status = "done"
                job.finished_at = time.time()
                job.downloaded = job.total
            elif job.requeue:
                job.status = "queued"  # picked straight back up with the new links
            elif job.pausing:
                job.status = "paused"
            else:
                job.status = "error"
                job.error = (download.errors or ["The download stopped early."])[0]
        except Exception as error:
            job.status = "error"
            job.error = str(error) or error.__class__.__name__
            job.rate = 0.0
        finally:
            job.pausing = False
            job.requeue = False
            job.download = None

    def _single(self, job: Job, adapter: mp.Adapter) -> None:
        """Servers that refuse range requests get one plain stream."""
        session = mp.make_session(adapter.ip)
        try:
            response = session.get(job.remote.url, stream=True,
                                   allow_redirects=True, timeout=(10, 30))
            response.raise_for_status()
            done = 0
            last = time.monotonic()
            marker = 0
            with open(job.target, "wb") as handle:
                for block in response.iter_content(mp.BLOCK):
                    handle.write(block)
                    done += len(block)
                    job.downloaded = done
                    now = time.monotonic()
                    if now - last >= 0.4:
                        job.rate = (done - marker) / (now - last)
                        last, marker = now, done
            job.shares = {adapter.name: done}
            job.total = job.total or done
            job.downloaded = done
            job.rate = 0.0
            job.status = "done"
            job.finished_at = time.time()
        finally:
            session.close()

    # -- sampling for the graph ------------------------------------------
    def _sampler(self) -> None:
        while True:
            time.sleep(SAMPLE_INTERVAL)
            with self.lock:
                per_network = {name: 0.0 for name in self.networks}
                for job in self.jobs.values():
                    if job.status != "downloading":
                        continue
                    if job.download:
                        job.downloaded = job.download.downloaded
                        job.rate = sum(a.rate for a in job.download.adapters)
                        for adapter in job.download.adapters:
                            if adapter.name in per_network:
                                per_network[adapter.name] += adapter.rate
                            job.shares[adapter.name] = (
                                job.share_base.get(adapter.name, 0) + adapter.downloaded)
                    else:
                        # single-stream fallback reports its own rate
                        for name in job.shares:
                            if name in per_network:
                                per_network[name] += job.rate
                self.history.append({"t": time.time(), "networks": per_network})

    def snapshot(self) -> dict:
        with self.lock:
            jobs = [self.jobs[i].as_dict() for i in self.order if i in self.jobs]
            networks = []
            latest = self.history[-1]["networks"] if self.history else {}
            for name, cfg in self.networks.items():
                networks.append({
                    "name": name,
                    "ip": cfg.get("ip", ""),
                    "colour": cfg.get("colour"),
                    "enabled": cfg.get("enabled", True),
                    "present": cfg.get("present", False),
                    "limit": cfg.get("limit"),
                    "rate": latest.get(name, 0.0),
                })
            return {
                "networks": networks,
                "jobs": jobs,
                "history": list(self.history),
                "folder": str(self.folder),
                "connections": self.connections,
                "combined": sum(latest.values()) if latest else 0.0,
            }


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "multipath-ui"
    manager: Manager

    def log_message(self, *args):
        pass

    # -- plumbing --------------------------------------------------------
    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def _json(self, payload, code: int = 200) -> None:
        self._send(code, json.dumps(payload).encode("utf-8"), "application/json")

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8")) or {}
        except Exception:
            return {}

    # -- routes ----------------------------------------------------------
    def do_GET(self) -> None:
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif path == "/api/state":
            self._json(self.manager.snapshot())
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        body = self._body()
        manager = self.manager

        if path == "/api/add":
            url = (body.get("url") or "").strip()
            if not re.match(r"^https?://", url, re.I):
                return self._json({"error": "That needs to be an http or https link."}, 400)
            job = manager.add(url)
            return self._json({"id": job.id})

        match = re.fullmatch(r"/api/jobs/([0-9a-f]+)/(pause|resume|remove)", path)
        if match:
            job_id, action = match.groups()
            if not manager.get(job_id):
                return self._json({"error": "not found"}, 404)
            if action == "pause":
                manager.pause(job_id)
            elif action == "resume":
                manager.resume(job_id)
            else:
                threading.Thread(
                    target=manager.remove,
                    args=(job_id, bool(body.get("delete_file"))),
                    daemon=True,
                ).start()
            return self._json({"ok": True})

        if path == "/api/network":
            name = body.get("name") or ""
            limit = body.get("limit", ...)
            if limit is not ...:
                if limit in (None, "", 0, "0"):
                    limit = None
                else:
                    try:
                        limit = mp.parse_size(str(limit))
                    except Exception:
                        return self._json({"error": "Use a size like 500KB or 2MB."}, 400)
            manager.set_network(name, body.get("enabled"), limit)
            return self._json({"ok": True})

        if path == "/api/settings":
            folder = body.get("folder")
            if folder:
                candidate = Path(folder).expanduser()
                if not candidate.is_dir():
                    return self._json({"error": f"No folder at {candidate}"}, 400)
                manager.folder = candidate
            if body.get("connections"):
                try:
                    manager.connections = max(1, min(16, int(body["connections"])))
                except (TypeError, ValueError):
                    pass
            manager.save_config()
            return self._json({"ok": True})

        if path == "/api/rescan":
            manager.refresh_networks()
            return self._json({"ok": True})

        self._json({"error": "not found"}, 404)


def free_port(preferred: int) -> int:
    with socket.socket() as probe:
        try:
            probe.bind(("127.0.0.1", preferred))
            return preferred
        except OSError:
            pass
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Local web UI for multipath.")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--folder", help="where to save files")
    parser.add_argument("--connections", type=int, default=4)
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()

    folder = Path(args.folder).expanduser() if args.folder else default_folder()
    if not folder.is_dir():
        print(f"No folder at {folder}")
        return 1

    Handler.manager = Manager(folder, args.connections)
    port = free_port(args.port)
    # 127.0.0.1 only: this controls downloads, it has no business on the network.
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True

    url = f"http://127.0.0.1:{port}/"
    print(f"multipath is running at {url}")
    print(f"Saving to {folder}")
    print("Press Ctrl+C to stop.")
    if not args.no_browser:
        threading.Thread(target=lambda: (time.sleep(0.6), webbrowser.open(url)),
                         daemon=True).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    return 0


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>multipath</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 22 18'%3E%3Cpath d='M1 2 C 9 2, 9 9, 17 9' stroke='%234CC2FF' stroke-width='2' fill='none'/%3E%3Cpath d='M1 9 H 17' stroke='%236BE4A8' stroke-width='2' fill='none'/%3E%3Cpath d='M1 16 C 9 16, 9 9, 17 9' stroke='%23FFB860' stroke-width='2' fill='none'/%3E%3Ccircle cx='18.6' cy='9' r='2.4' fill='%23E8EFF7'/%3E%3C/svg%3E">
<style>
  /* No webfonts on purpose: this is a local tool, so it must look the same
     offline and has no business fetching anything when you open it. */
  :root {
    --ground: #0E141B;
    --panel: #151D27;
    --raised: #1B242F;
    --edge: #26323F;
    --edge-soft: #1E2833;
    --text: #E8EFF7;
    --muted: #8EA1B5;
    --faint: #5F7389;
    --bad: #FF8A98;
    --sans: "Segoe UI Variable Text", "Segoe UI", system-ui, -apple-system, sans-serif;
    --mono: "Cascadia Mono", Consolas, ui-monospace, SFMono-Regular, Menlo, monospace;
  }

  * { box-sizing: border-box; }

  html, body { margin: 0; }

  body {
    background: var(--ground);
    color: var(--text);
    font-family: var(--sans);
    font-size: 14px;
    line-height: 1.5;
    -webkit-font-smoothing: antialiased;
  }

  .num { font-family: var(--mono); font-variant-numeric: tabular-nums; }

  /* ---------- top bar ---------- */

  header {
    display: flex;
    align-items: center;
    gap: 20px;
    padding: 14px 24px;
    border-bottom: 1px solid var(--edge-soft);
    background: var(--ground);
    position: sticky;
    top: 0;
    z-index: 10;
  }

  .wordmark {
    font-weight: 600;
    font-size: 15px;
    letter-spacing: -0.01em;
    display: flex;
    align-items: center;
    gap: 9px;
    flex: none;
  }

  /* Three strands converging into one: the wordmark carries the product idea. */
  .wordmark svg { display: block; }

  form.add {
    display: flex;
    gap: 8px;
    flex: 1;
    max-width: 680px;
  }

  input[type=url], input.limit, input.folder {
    font: inherit;
    color: var(--text);
    background: var(--panel);
    border: 1px solid var(--edge);
    border-radius: 7px;
    padding: 8px 12px;
  }

  input[type=url] { flex: 1; min-width: 0; }
  input::placeholder { color: var(--faint); }

  input:focus-visible, button:focus-visible, .toggle:focus-visible {
    outline: 2px solid #4CC2FF;
    outline-offset: 2px;
  }

  button {
    font: inherit;
    font-weight: 500;
    border-radius: 7px;
    border: 1px solid var(--edge);
    background: var(--raised);
    color: var(--text);
    padding: 8px 14px;
    cursor: pointer;
  }

  button:hover { border-color: #33424F; }
  button.primary {
    background: var(--text);
    color: #0E141B;
    border-color: var(--text);
    flex: none;
  }
  button.primary:hover { background: #fff; }
  button.quiet {
    background: none;
    border-color: transparent;
    color: var(--muted);
    padding: 5px 9px;
  }
  button.quiet:hover { color: var(--text); background: var(--raised); }

  /* ---------- hero: the stacked throughput graph ---------- */

  .hero {
    padding: 26px 24px 18px;
    border-bottom: 1px solid var(--edge-soft);
  }

  .hero-top {
    display: flex;
    align-items: baseline;
    gap: 14px;
    margin-bottom: 14px;
  }

  /* Sans, not mono, at display size: tabular-nums already stops the digits
     jittering, and mono fallbacks space badly this large. */
  .combined {
    font-variant-numeric: tabular-nums;
    font-size: 40px;
    font-weight: 450;
    line-height: 1;
    letter-spacing: -0.02em;
  }
  .combined.idle { color: var(--faint); }
  .combined-unit { font-size: 18px; color: var(--muted); }
  .hero-caption { color: var(--muted); }

  .graph-wrap {
    position: relative;
    height: 132px;
    border-bottom: 1px solid var(--edge);
  }
  .graph-wrap svg { display: block; width: 100%; height: 100%; }
  .graph-empty {
    position: absolute;
    inset: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--faint);
  }
  .axis-label {
    position: absolute;
    right: 0;
    top: 0;
    color: var(--faint);
    font-family: var(--mono);
    font-size: 11px;
  }

  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 6px 22px;
    margin-top: 12px;
  }
  .legend-item { display: flex; align-items: center; gap: 8px; }
  .legend-item.off { opacity: 0.4; }
  .swatch { width: 9px; height: 9px; border-radius: 2px; flex: none; }
  .legend-rate { color: var(--muted); font-size: 13px; }

  /* ---------- body ---------- */

  main {
    display: grid;
    grid-template-columns: 274px 1fr;
    align-items: start;
    gap: 0;
    min-height: 50vh;
  }

  .rail {
    border-right: 1px solid var(--edge-soft);
    padding: 20px 20px 24px;
    position: sticky;
    top: 61px;
  }

  h2 {
    font-size: 13px;
    font-weight: 600;
    color: var(--muted);
    margin: 0 0 14px;
  }

  .net { padding: 11px 0; border-top: 1px solid var(--edge-soft); }
  .net:first-of-type { border-top: none; }
  .net.absent { opacity: 0.45; }

  .net-head { display: flex; align-items: center; gap: 10px; }
  .net-name { font-weight: 450; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .net-rate { font-family: var(--mono); font-size: 12.5px; color: var(--muted); font-variant-numeric: tabular-nums; }

  .toggle {
    width: 30px; height: 18px;
    border-radius: 9px;
    border: 1px solid var(--edge);
    background: var(--raised);
    padding: 0;
    position: relative;
    flex: none;
    cursor: pointer;
    transition: background .14s, border-color .14s;
  }
  .toggle .knob {
    position: absolute;
    top: 2px; left: 2px;
    width: 12px; height: 12px;
    border-radius: 50%;
    background: var(--faint);
    transition: transform .14s, background .14s;
  }
  .toggle[aria-checked=true] .knob { transform: translateX(12px); }
  .toggle:disabled { cursor: not-allowed; }

  .net-meta {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 7px 0 0 40px;
  }
  .net-ip { font-family: var(--mono); font-size: 11.5px; color: var(--faint); }
  input.limit {
    width: 78px;
    padding: 3px 7px;
    font-size: 12px;
    font-family: var(--mono);
    border-radius: 5px;
  }

  .rail-foot { margin-top: 22px; padding-top: 18px; border-top: 1px solid var(--edge-soft); }
  .field { margin-bottom: 12px; }
  .field label { display: block; color: var(--muted); font-size: 12.5px; margin-bottom: 5px; }
  input.folder { width: 100%; font-size: 12.5px; padding: 6px 9px; }
  .conn-row { display: flex; align-items: center; gap: 9px; }
  .conn-row input { width: 56px; text-align: center; }

  /* ---------- files ---------- */

  .files { padding: 20px 24px 40px; }

  .file {
    padding: 16px 0;
    border-top: 1px solid var(--edge-soft);
  }
  .file:first-of-type { border-top: none; padding-top: 4px; }

  .file-head { display: flex; align-items: baseline; gap: 12px; }
  .file-name {
    font-weight: 450;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    flex: 1;
    min-width: 0;
  }
  .file-actions { display: flex; gap: 2px; flex: none; }

  .track {
    height: 5px;
    background: var(--edge-soft);
    border-radius: 3px;
    overflow: hidden;
    margin: 11px 0 8px;
  }
  .fill { height: 100%; background: var(--text); border-radius: 3px; transition: width .3s linear; }
  .fill.done { background: #6BE4A8; }
  .fill.paused { background: var(--faint); }
  .fill.error { background: var(--bad); }
  .track.indeterminate .fill {
    width: 34% !important;
    animation: slide 1.3s ease-in-out infinite;
  }
  @keyframes slide { 0%,100% { margin-left: 0; } 50% { margin-left: 66%; } }

  .file-stats { color: var(--muted); font-size: 13px; display: flex; gap: 14px; flex-wrap: wrap; }
  .file-stats .num { color: var(--text); }
  .file-error { color: var(--bad); font-size: 13px; margin-top: 7px; }

  /* which network contributed what, for this one file */
  .shares { display: flex; height: 3px; gap: 1px; margin-top: 10px; }
  .shares span { border-radius: 1px; }

  .empty { padding: 56px 0; text-align: center; color: var(--faint); }
  .empty strong { display: block; color: var(--muted); font-weight: 450; margin-bottom: 4px; }

  .toast {
    position: fixed;
    left: 50%; bottom: 22px;
    transform: translateX(-50%);
    background: var(--raised);
    border: 1px solid var(--edge);
    color: var(--text);
    padding: 9px 15px;
    border-radius: 8px;
    box-shadow: 0 8px 28px rgba(0,0,0,.45);
  }
  .toast.bad { border-color: #5E2A33; color: var(--bad); }

  @media (max-width: 760px) {
    main { grid-template-columns: 1fr; }
    .rail { position: static; border-right: none; border-bottom: 1px solid var(--edge-soft); }
    header { flex-wrap: wrap; }
    .combined { font-size: 32px; }
  }

  @media (prefers-reduced-motion: reduce) {
    * { transition: none !important; animation: none !important; }
  }
</style>
</head>
<body>

<header>
  <div class="wordmark">
    <svg width="22" height="18" viewBox="0 0 22 18" fill="none" aria-hidden="true">
      <path d="M1 2 C 9 2, 9 9, 17 9" stroke="#4CC2FF" stroke-width="1.6" fill="none"/>
      <path d="M1 9 H 17" stroke="#6BE4A8" stroke-width="1.6" fill="none"/>
      <path d="M1 16 C 9 16, 9 9, 17 9" stroke="#FFB860" stroke-width="1.6" fill="none"/>
      <circle cx="18.6" cy="9" r="2.2" fill="#E8EFF7"/>
    </svg>
    multipath
  </div>
  <form class="add" id="add-form">
    <input type="url" id="url" placeholder="Paste a download link" autocomplete="off" spellcheck="false" required>
    <button class="primary" type="submit">Add download</button>
  </form>
</header>

<section class="hero">
  <div class="hero-top">
    <div class="combined idle"><span id="combined">0</span><span class="combined-unit" id="combined-unit"> B/s</span></div>
    <div class="hero-caption" id="hero-caption">idle</div>
  </div>
  <div class="graph-wrap">
    <svg id="graph" viewBox="0 0 1000 132" preserveAspectRatio="none" aria-hidden="true"></svg>
    <div class="axis-label" id="axis-label"></div>
    <div class="graph-empty" id="graph-empty">Throughput appears here, one band per network</div>
  </div>
  <div class="legend" id="legend"></div>
</section>

<main>
  <aside class="rail">
    <h2>Networks</h2>
    <div id="networks"></div>
    <button class="quiet" id="rescan" style="margin-top:10px">Rescan networks</button>

    <div class="rail-foot">
      <div class="field">
        <label for="folder">Save to</label>
        <input class="folder" id="folder" spellcheck="false">
      </div>
      <div class="field conn-row">
        <label for="connections" style="margin:0;flex:1">Connections each</label>
        <input class="limit" id="connections" type="number" min="1" max="16">
      </div>
    </div>
  </aside>

  <section class="files">
    <h2>Files</h2>
    <div id="files"></div>
  </section>
</main>

<script>
const $ = (id) => document.getElementById(id);
let editing = null;   // don't overwrite a field while it is being typed in
let netOrder = [];

function bytes(n) {
  if (!n) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (Math.abs(n) >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return (i === 0 ? n.toFixed(0) : n.toFixed(1)) + " " + units[i];
}
function splitRate(n) {
  if (!n || n < 1) return ["0", " B/s"];
  const units = ["B", "KB", "MB", "GB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
  return [i === 0 ? n.toFixed(0) : n.toFixed(1), " " + units[i] + "/s"];
}
function clock(s) {
  if (s == null || !isFinite(s)) return null;
  s = Math.max(0, Math.round(s));
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = s % 60;
  const pad = (v) => String(v).padStart(2, "0");
  return h ? `${h}:${pad(m)}:${pad(x)}` : `${pad(m)}:${pad(x)}`;
}

function toast(message, bad) {
  const el = document.createElement("div");
  el.className = "toast" + (bad ? " bad" : "");
  el.textContent = message;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 3800);
}

async function post(path, body) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || "That didn't work.");
  return data;
}

/* ---------- the stacked graph ---------- */

function drawGraph(history, networks) {
  const svg = $("graph");
  const order = networks.filter(n => n.present).map(n => n.name);
  const colour = {};
  networks.forEach(n => colour[n.name] = n.colour);

  const samples = history.slice(-150);
  const totals = samples.map(s => order.reduce((a, n) => a + (s.networks[n] || 0), 0));
  const peak = Math.max(...totals, 0);

  if (!samples.length || peak <= 0) {
    svg.innerHTML = "";
    $("graph-empty").style.display = "flex";
    $("axis-label").textContent = "";
    return;
  }
  $("graph-empty").style.display = "none";

  const W = 1000, H = 132;
  const top = peak * 1.15;
  $("axis-label").textContent = bytes(top) + "/s";

  const x = (i) => (samples.length === 1 ? W : (i / (samples.length - 1)) * W);
  const y = (v) => H - (v / top) * H;

  // Cumulative stack, so the bands sum to the real combined throughput.
  const below = samples.map(() => 0);
  let out = "";
  for (const name of order) {
    const upper = samples.map((s, i) => below[i] + (s.networks[name] || 0));
    let d = `M ${x(0)} ${y(below[0])}`;
    for (let i = 0; i < samples.length; i++) d += ` L ${x(i)} ${y(upper[i])}`;
    for (let i = samples.length - 1; i >= 0; i--) d += ` L ${x(i)} ${y(below[i])}`;
    d += " Z";
    let line = `M ${x(0)} ${y(upper[0])}`;
    for (let i = 1; i < samples.length; i++) line += ` L ${x(i)} ${y(upper[i])}`;
    const c = colour[name] || "#4CC2FF";
    out += `<path d="${d}" fill="${c}" fill-opacity="0.26"/>`;
    out += `<path d="${line}" fill="none" stroke="${c}" stroke-width="1.5"
             vector-effect="non-scaling-stroke" stroke-linejoin="round"/>`;
    for (let i = 0; i < samples.length; i++) below[i] = upper[i];
  }
  svg.innerHTML = out;
}

/* ---------- networks rail ---------- */

function renderNetworks(networks) {
  const host = $("networks");
  const signature = networks.map(n => n.name + n.present + n.enabled).join("|");
  if (host.dataset.signature !== signature) {
    host.dataset.signature = signature;
    host.innerHTML = "";
    for (const net of networks) {
      const row = document.createElement("div");
      row.className = "net" + (net.present ? "" : " absent");
      row.dataset.name = net.name;
      row.innerHTML = `
        <div class="net-head">
          <button class="toggle" role="switch" aria-checked="${net.enabled}"
                  aria-label="Use ${net.name}" ${net.present ? "" : "disabled"}
                  style="${net.enabled && net.present ? `background:${net.colour}33;border-color:${net.colour}` : ""}">
            <span class="knob" style="${net.enabled && net.present ? `background:${net.colour}` : ""}"></span>
          </button>
          <span class="net-name">${net.name}</span>
          <span class="net-rate" data-rate></span>
        </div>
        <div class="net-meta">
          <span class="net-ip">${net.present ? net.ip : "not connected"}</span>
          <input class="limit" data-limit placeholder="no limit"
                 title="Cap this network, e.g. 500KB or 2MB. Empty means no cap."
                 aria-label="Speed limit for ${net.name}" spellcheck="false">
        </div>`;
      host.appendChild(row);

      row.querySelector(".toggle").addEventListener("click", async (event) => {
        const button = event.currentTarget;
        const next = button.getAttribute("aria-checked") !== "true";
        try { await post("/api/network", { name: net.name, enabled: next }); refresh(); }
        catch (error) { toast(error.message, true); }
      });

      const limit = row.querySelector("[data-limit]");
      limit.addEventListener("focus", () => editing = limit);
      limit.addEventListener("blur", () => { if (editing === limit) editing = null; });
      limit.addEventListener("change", async () => {
        try {
          await post("/api/network", { name: net.name, limit: limit.value.trim() });
          toast(limit.value.trim() ? `${net.name} capped at ${limit.value.trim()}` : `${net.name} uncapped`);
        } catch (error) { toast(error.message, true); limit.value = ""; }
        editing = null;
        refresh();
      });
    }
  }
  for (const net of networks) {
    const row = host.querySelector(`[data-name="${CSS.escape(net.name)}"]`);
    if (!row) continue;
    row.querySelector("[data-rate]").textContent =
      net.present && net.enabled && net.rate > 1 ? splitRate(net.rate).join("") : "";
    const limit = row.querySelector("[data-limit]");
    // Show it as the rate it actually is; parse_size accepts the "/s" back.
    if (editing !== limit) limit.value = net.limit ? bytes(net.limit).replace(".0", "") + "/s" : "";
  }
}

/* ---------- legend ---------- */

function renderLegend(networks) {
  const live = networks.filter(n => n.present);
  $("legend").innerHTML = live.map(n => `
    <div class="legend-item ${n.enabled ? "" : "off"}">
      <span class="swatch" style="background:${n.colour}"></span>
      <span>${n.name}</span>
      <span class="legend-rate">${n.enabled ? (n.rate > 1 ? splitRate(n.rate).join("") : "idle") : "off"}</span>
    </div>`).join("");
}

/* ---------- files ---------- */

const LABEL = {
  queued: "Waiting", checking: "Checking the link", downloading: "Downloading",
  paused: "Paused", done: "Finished", error: "Stopped",
};

function renderFiles(jobs, networks) {
  const host = $("files");
  const colour = {};
  networks.forEach(n => colour[n.name] = n.colour);

  if (!jobs.length) {
    host.innerHTML = `<div class="empty"><strong>No downloads yet</strong>
      Paste a link above to start one.</div>`;
    return;
  }
  if (host.querySelector(".empty")) host.innerHTML = "";

  const seen = new Set();
  for (const job of jobs) {
    seen.add(job.id);
    let row = host.querySelector(`[data-job="${job.id}"]`);
    if (!row) {
      row = document.createElement("div");
      row.className = "file";
      row.dataset.job = job.id;
      row.innerHTML = `
        <div class="file-head">
          <span class="file-name"></span>
          <span class="file-actions"></span>
        </div>
        <div class="track"><div class="fill"></div></div>
        <div class="file-stats"></div>
        <div class="shares"></div>
        <div class="file-error" style="display:none"></div>`;
      host.appendChild(row);
    }

    row.querySelector(".file-name").textContent = job.name;
    row.querySelector(".file-name").title = job.url;

    const pct = job.total ? Math.min(100, (job.downloaded / job.total) * 100) : 0;
    const track = row.querySelector(".track");
    const fill = row.querySelector(".fill");
    const indeterminate = job.status === "downloading" && !job.total;
    track.classList.toggle("indeterminate", indeterminate);
    fill.style.width = indeterminate ? "" : pct + "%";
    fill.className = "fill" + (job.status === "done" ? " done"
      : job.status === "paused" ? " paused"
      : job.status === "error" ? " error" : "");

    const bits = [LABEL[job.status] || job.status];
    if (job.total) bits.push(`<span class="num">${bytes(job.downloaded)}</span> of <span class="num">${bytes(job.total)}</span>`);
    else if (job.downloaded) bits.push(`<span class="num">${bytes(job.downloaded)}</span>`);
    if (job.status === "downloading" && job.rate > 1) bits.push(`<span class="num">${splitRate(job.rate).join("")}</span>`);
    const eta = clock(job.eta);
    if (eta && job.status === "downloading") bits.push(`<span class="num">${eta}</span> left`);
    row.querySelector(".file-stats").innerHTML = bits.map(b => `<span>${b}</span>`).join("");

    // Per-file attribution: the product's claim, repeated at file level.
    const shares = Object.entries(job.shares || {}).filter(([, v]) => v > 0);
    const sum = shares.reduce((a, [, v]) => a + v, 0);
    row.querySelector(".shares").innerHTML = sum
      ? shares.map(([name, v]) =>
          `<span style="width:${(v / sum) * 100}%;background:${colour[name] || "#4CC2FF"}"
                 title="${name}: ${bytes(v)}"></span>`).join("")
      : "";

    const error = row.querySelector(".file-error");
    error.style.display = job.error ? "block" : "none";
    error.textContent = job.error || "";

    const actions = row.querySelector(".file-actions");
    const wanted = (job.status === "downloading" || job.status === "queued" || job.status === "checking")
      ? "pause" : (job.status === "paused" || job.status === "error") ? "resume" : "none";
    if (actions.dataset.wanted !== wanted) {
      actions.dataset.wanted = wanted;
      actions.innerHTML = "";
      if (wanted !== "none") {
        const button = document.createElement("button");
        button.className = "quiet";
        button.textContent = wanted === "pause" ? "Pause" : "Resume";
        button.onclick = () => post(`/api/jobs/${job.id}/${wanted}`).then(refresh)
          .catch(e => toast(e.message, true));
        actions.appendChild(button);
      }
      const remove = document.createElement("button");
      remove.className = "quiet";
      remove.textContent = "Remove";
      remove.onclick = () => {
        const unfinished = job.status !== "done";
        const drop = unfinished && confirm(`Delete the partly downloaded ${job.name} as well?`);
        post(`/api/jobs/${job.id}/remove`, { delete_file: drop }).then(refresh)
          .catch(e => toast(e.message, true));
      };
      actions.appendChild(remove);
    }
  }
  for (const row of [...host.querySelectorAll("[data-job]")]) {
    if (!seen.has(row.dataset.job)) row.remove();
  }
}

/* ---------- poll ---------- */

async function refresh() {
  let data;
  try {
    data = await (await fetch("/api/state")).json();
  } catch { return; }

  netOrder = data.networks;
  const [value, unit] = splitRate(data.combined);
  $("combined").textContent = value;
  $("combined-unit").textContent = unit;
  document.querySelector(".combined").classList.toggle("idle", data.combined < 1);

  const live = data.jobs.filter(j => j.status === "downloading").length;
  const using = data.networks.filter(n => n.present && n.enabled && n.rate > 1).length;
  $("hero-caption").textContent = !live ? "idle"
    : using === 0 ? "starting"
    : `across ${using} ${using === 1 ? "network" : "networks"}`;

  drawGraph(data.history, data.networks);
  renderLegend(data.networks);
  renderNetworks(data.networks);
  renderFiles(data.jobs, data.networks);

  if (editing !== $("folder")) $("folder").value = data.folder;
  if (editing !== $("connections")) $("connections").value = data.connections;
}

$("add-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const input = $("url");
  const url = input.value.trim();
  if (!url) return;
  try {
    await post("/api/add", { url });
    input.value = "";
    refresh();
  } catch (error) { toast(error.message, true); }
});

$("rescan").addEventListener("click", () =>
  post("/api/rescan").then(refresh).catch(e => toast(e.message, true)));

for (const id of ["folder", "connections"]) {
  const el = $(id);
  el.addEventListener("focus", () => editing = el);
  el.addEventListener("blur", () => { if (editing === el) editing = null; });
  el.addEventListener("change", async () => {
    try {
      await post("/api/settings", { [id]: el.value.trim() });
      toast(id === "folder" ? "Saving to " + el.value.trim() : "Using " + el.value + " connections per network");
    } catch (error) { toast(error.message, true); }
    editing = null;
    refresh();
  });
}

refresh();
setInterval(refresh, 500);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    sys.exit(main())
