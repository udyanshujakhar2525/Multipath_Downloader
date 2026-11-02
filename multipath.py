#!/usr/bin/env python3
"""
multipath.py - download one file over several network connections at once.

Splits a file into range-chunks and pulls them in parallel over every usable
network adapter (Ethernet, Wi-Fi, phone tethered over USB). Each connection is
bound to a specific adapter's source IP, so the traffic genuinely leaves via
that link. Chunks are handed out on demand, so fast adapters naturally take
more of the file without any manual tuning.

Requirements:
    pip install requests psutil

Usage:
    python multipath.py --list
    python multipath.py <url>
    python multipath.py <url> -o C:\\Downloads\\file.iso
    python multipath.py <url> --only "Ethernet,Wi-Fi"
    python multipath.py <url> --exclude "iPhone USB"
    python multipath.py <url> --limit "iPhone USB=500KB"
    python multipath.py <url> --connections 6

Ctrl+C saves progress; re-run the same command to resume.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import unquote, urlparse

try:
    import psutil
except ImportError:
    sys.exit("Missing dependency. Run:  pip install psutil requests")

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    sys.exit("Missing dependency. Run:  pip install psutil requests")


USER_AGENT = "multipath/1.0"
BLOCK = 64 * 1024
MIN_CHUNK = 1 * 1024 * 1024
MAX_CHUNK = 16 * 1024 * 1024
TARGET_CHUNKS = 256  # plenty of chunks so work-stealing can balance the links
PROBE_TIMEOUT = 8
MAX_CHUNK_FAILURES = 3

# Adapters that exist on most Windows boxes but never reach the internet.
VIRTUAL_HINTS = (
    "loopback", "vmware", "virtualbox", "vethernet", "hyper-v", "docker",
    "npcap", "tap-windows", "bluetooth", "wsl", "teredo", "isatap",
)


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------

def enable_ansi() -> bool:
    """Turn on VT escape handling in the Windows console. Returns success."""
    if os.name != "nt":
        return sys.stdout.isatty()
    if not sys.stdout.isatty():
        return False
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        # ENABLE_VIRTUAL_TERMINAL_PROCESSING
        return bool(kernel32.SetConsoleMode(handle, mode.value | 0x0004))
    except Exception:
        return False


def human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def human_time(seconds: float) -> str:
    if seconds is None or seconds != seconds or seconds in (float("inf"),):
        return "--:--"
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def parse_size(text: str) -> int:
    """'500KB' / '2M' / '1.5 MB' -> bytes per second."""
    match = re.fullmatch(r"\s*([\d.]+)\s*([KMG]?)B?(?:/s)?\s*", text, re.I)
    if not match:
        raise argparse.ArgumentTypeError(f"cannot read size: {text!r}")
    value = float(match.group(1))
    return int(value * {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3}[match.group(2).upper()])


class RateLimiter:
    """Token bucket. Useful for capping a metered tethered connection."""

    def __init__(self, rate: int | None):
        self.rate = rate
        # Start empty, not full: a full bucket lets the first second run well
        # over the cap, which matters when the cap exists to protect a data plan.
        self.allowance = 0.0
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def consume(self, amount: int) -> None:
        if not self.rate:
            return
        while True:
            with self.lock:
                now = time.monotonic()
                self.allowance = min(
                    float(self.rate), self.allowance + (now - self.last) * self.rate
                )
                self.last = now
                if self.allowance >= amount:
                    self.allowance -= amount
                    return
                wait = (amount - self.allowance) / self.rate
            time.sleep(min(wait, 0.25))


# --------------------------------------------------------------------------
# network adapters
# --------------------------------------------------------------------------

@dataclass
class Adapter:
    name: str
    ip: str
    limiter: RateLimiter = field(default_factory=lambda: RateLimiter(None))
    downloaded: int = 0
    rate: float = 0.0
    active: int = 0
    note: str = ""


class BoundAdapter(HTTPAdapter):
    """requests adapter that pins every socket to one local source IP."""

    def __init__(self, source_ip: str, **kwargs):
        self.source_ip = source_ip
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["source_address"] = (self.source_ip, 0)
        super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs["source_address"] = (self.source_ip, 0)
        return super().proxy_manager_for(proxy, **proxy_kwargs)


def make_session(source_ip: str | None) -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT
    if source_ip:
        adapter = BoundAdapter(source_ip, pool_connections=4, pool_maxsize=8)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    # Proxies silently defeat source binding: everything would exit one link.
    session.trust_env = False
    return session


def discover_adapters() -> list[Adapter]:
    found: list[Adapter] = []
    stats = psutil.net_if_stats()
    import socket as _socket

    for name, addresses in psutil.net_if_addrs().items():
        info = stats.get(name)
        if not info or not info.isup:
            continue
        lowered = name.lower()
        if any(hint in lowered for hint in VIRTUAL_HINTS):
            continue
        for address in addresses:
            if address.family != _socket.AF_INET:
                continue
            ip = address.address
            if ip.startswith(("127.", "169.254.")):
                continue
            found.append(Adapter(name=name, ip=ip))
            break
    return found


# --------------------------------------------------------------------------
# remote file metadata
# --------------------------------------------------------------------------

@dataclass
class RemoteFile:
    url: str
    size: int | None
    supports_ranges: bool
    etag: str | None
    filename: str


def filename_from(response: requests.Response, url: str) -> str:
    disposition = response.headers.get("Content-Disposition", "")
    match = re.search(r"filename\*=UTF-8''([^;]+)", disposition, re.I)
    if match:
        return unquote(match.group(1).strip().strip('"'))
    match = re.search(r'filename="?([^";]+)"?', disposition, re.I)
    if match:
        return unquote(match.group(1).strip())
    name = Path(unquote(urlparse(url).path)).name
    return name or "download.bin"


def probe_remote(session: requests.Session, url: str) -> RemoteFile:
    """Resolve redirects and learn size / range support with one tiny request."""
    response = session.get(
        url, headers={"Range": "bytes=0-0"}, stream=True,
        allow_redirects=True, timeout=PROBE_TIMEOUT,
    )
    try:
        response.raise_for_status()
        final_url = response.url
        etag = response.headers.get("ETag")
        size: int | None = None
        supports_ranges = False

        if response.status_code == 206:
            content_range = response.headers.get("Content-Range", "")
            match = re.search(r"/(\d+)\s*$", content_range)
            if match:
                size = int(match.group(1))
                supports_ranges = True
        else:
            length = response.headers.get("Content-Length")
            if length and length.isdigit():
                size = int(length)

        if size is not None and size <= 0:
            size = None
        return RemoteFile(final_url, size, supports_ranges, etag, filename_from(response, final_url))
    finally:
        response.close()


def usable_adapters(adapters: list[Adapter], url: str) -> list[Adapter]:
    """Keep only the adapters that can actually reach the resolved URL."""
    results: dict[str, Exception | None] = {}
    lock = threading.Lock()

    def check(adapter: Adapter) -> None:
        session = make_session(adapter.ip)
        try:
            response = session.get(
                url, headers={"Range": "bytes=0-0"}, stream=True,
                allow_redirects=True, timeout=PROBE_TIMEOUT,
            )
            response.raise_for_status()
            response.close()
            with lock:
                results[adapter.name] = None
        except Exception as error:  # unreachable, no route, DNS fail, blocked
            with lock:
                results[adapter.name] = error
        finally:
            session.close()

    threads = [threading.Thread(target=check, args=(a,), daemon=True) for a in adapters]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(PROBE_TIMEOUT + 4)

    good = []
    for adapter in adapters:
        error = results.get(adapter.name, RuntimeError("probe timed out"))
        if error is None:
            good.append(adapter)
        else:
            reason = type(error).__name__ if not isinstance(error, requests.RequestException) else "unreachable"
            print(f"  skipping {adapter.name} ({adapter.ip}) - {reason}")
    return good


# --------------------------------------------------------------------------
# resume state
# --------------------------------------------------------------------------

class State:
    def __init__(self, path: Path):
        self.path = path
        self.done: set[int] = set()
        self.meta: dict = {}
        self.lock = threading.Lock()
        self.dirty = False

    def load(self, url: str, size: int, chunk_size: int, etag: str | None) -> bool:
        if not self.path.exists():
            return False
        try:
            data = json.loads(self.path.read_text("utf-8"))
        except Exception:
            return False
        same = (
            data.get("size") == size
            and data.get("chunk_size") == chunk_size
            and data.get("etag") == etag
        )
        if not same:
            return False
        self.done = set(data.get("done", []))
        self.meta = {"url": url, "size": size, "chunk_size": chunk_size, "etag": etag}
        return bool(self.done)

    def start(self, url: str, size: int, chunk_size: int, etag: str | None) -> None:
        self.meta = {"url": url, "size": size, "chunk_size": chunk_size, "etag": etag}

    def mark(self, index: int) -> None:
        with self.lock:
            self.done.add(index)
            self.dirty = True

    def flush(self) -> None:
        with self.lock:
            if not self.dirty:
                return
            payload = dict(self.meta, done=sorted(self.done))
            self.dirty = False
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        try:
            tmp.write_text(json.dumps(payload), "utf-8")
            os.replace(tmp, self.path)
        except Exception:
            pass

    def clear(self) -> None:
        try:
            self.path.unlink()
        except OSError:
            pass


# --------------------------------------------------------------------------
# the download itself
# --------------------------------------------------------------------------

class Download:
    def __init__(self, remote: RemoteFile, adapters: list[Adapter], target: Path,
                 connections: int, chunk_size: int, state: State):
        self.remote = remote
        self.adapters = adapters
        self.target = target
        self.connections = connections
        self.chunk_size = chunk_size
        self.state = state
        self.total = remote.size or 0
        self.chunks = (self.total + chunk_size - 1) // chunk_size
        self.work: queue.Queue[int] = queue.Queue()
        self.downloaded = 0
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.errors: list[str] = []
        self.started = time.monotonic()

    # -- accounting ------------------------------------------------------
    def add_bytes(self, adapter: Adapter, amount: int) -> None:
        with self.lock:
            self.downloaded += amount
            adapter.downloaded += amount

    def rollback(self, adapter: Adapter, amount: int) -> None:
        if amount:
            with self.lock:
                self.downloaded -= amount
                adapter.downloaded -= amount

    # -- worker ----------------------------------------------------------
    def worker(self, adapter: Adapter) -> None:
        session = make_session(adapter.ip)
        handle = open(self.target, "r+b")
        failures = 0
        try:
            while not self.stop.is_set():
                try:
                    index = self.work.get_nowait()
                except queue.Empty:
                    return

                start = index * self.chunk_size
                end = min(start + self.chunk_size, self.total) - 1
                written = 0
                with self.lock:
                    adapter.active += 1
                try:
                    response = session.get(
                        self.remote.url,
                        headers={"Range": f"bytes={start}-{end}"},
                        stream=True, allow_redirects=True, timeout=(10, 30),
                    )
                    if response.status_code != 206:
                        raise RuntimeError(f"server ignored range (HTTP {response.status_code})")
                    served = response.headers.get("ETag")
                    if self.remote.etag and served and served != self.remote.etag:
                        raise RuntimeError("this link serves a different file on this network")

                    handle.seek(start)
                    for block in response.iter_content(BLOCK):
                        if self.stop.is_set():
                            raise RuntimeError("cancelled")
                        if not block:
                            continue
                        adapter.limiter.consume(len(block))
                        handle.write(block)
                        written += len(block)
                        self.add_bytes(adapter, len(block))
                    response.close()

                    if written != end - start + 1:
                        raise RuntimeError("short read")

                    handle.flush()
                    self.state.mark(index)
                    failures = 0
                except Exception as error:
                    self.rollback(adapter, written)
                    self.work.put(index)
                    failures += 1
                    if self.stop.is_set():
                        return
                    if failures >= MAX_CHUNK_FAILURES:
                        with self.lock:
                            adapter.note = str(error)[:48]
                            self.errors.append(f"{adapter.name}: {error}")
                        return
                    time.sleep(1.0 * failures)
                finally:
                    with self.lock:
                        adapter.active -= 1
        finally:
            handle.close()
            session.close()

    # -- display ---------------------------------------------------------
    def render(self, fancy: bool, first: bool) -> int:
        lines = []
        done = self.downloaded
        fraction = done / self.total if self.total else 0
        filled = int(fraction * 34)
        speed = sum(a.rate for a in self.adapters)
        eta = (self.total - done) / speed if speed > 1 else None

        lines.append(
            f"  [{'#' * filled}{'.' * (34 - filled)}] {fraction * 100:5.1f}%  "
            f"{human_bytes(done)} / {human_bytes(self.total)}  "
            f"{human_bytes(speed)}/s  ETA {human_time(eta)}"
        )
        lines.append("")
        peak = max([a.rate for a in self.adapters] + [1.0])
        width = max(len(a.name) for a in self.adapters)
        for adapter in self.adapters:
            bar = int((adapter.rate / peak) * 12)
            status = adapter.note or f"{adapter.active} conn"
            lines.append(
                f"  {adapter.name:<{width}}  {human_bytes(adapter.rate) + '/s':>11}  "
                f"[{'#' * bar}{'.' * (12 - bar)}]  {human_bytes(adapter.downloaded):>9}  {status}"
            )

        text = "\n".join(lines)
        if fancy and not first:
            sys.stdout.write(f"\x1b[{len(lines)}A\x1b[0J")
        sys.stdout.write(text + "\n")
        sys.stdout.flush()
        return len(lines)

    def monitor(self, fancy: bool) -> None:
        previous = {a.name: 0 for a in self.adapters}
        last = time.monotonic()
        first = True
        while not self.stop.is_set():
            time.sleep(0.4)
            now = time.monotonic()
            elapsed = max(now - last, 1e-6)
            last = now
            with self.lock:
                for adapter in self.adapters:
                    delta = adapter.downloaded - previous[adapter.name]
                    previous[adapter.name] = adapter.downloaded
                    instant = delta / elapsed
                    # light smoothing so the numbers don't jitter
                    adapter.rate = instant if first else adapter.rate * 0.6 + instant * 0.4
            self.render(fancy, first)
            first = False
            self.state.flush()

    # -- run -------------------------------------------------------------
    def run(self, fancy: bool) -> bool:
        for index in range(self.chunks):
            if index not in self.state.done:
                self.work.put(index)
        self.downloaded = len(self.state.done) * self.chunk_size
        self.downloaded = min(self.downloaded, self.total)

        monitor = threading.Thread(target=self.monitor, args=(fancy,), daemon=True)
        monitor.start()

        threads = []
        for adapter in self.adapters:
            for _ in range(self.connections):
                thread = threading.Thread(target=self.worker, args=(adapter,), daemon=True)
                thread.start()
                threads.append(thread)

        try:
            for thread in threads:
                while thread.is_alive():
                    thread.join(0.3)
        except KeyboardInterrupt:
            self.stop.set()
            for thread in threads:
                thread.join(2)
            self.state.flush()
            print("\nPaused. Run the same command again to resume.")
            return False

        self.stop.set()
        monitor.join(1)
        self.state.flush()
        return len(self.state.done) == self.chunks


def single_stream(remote: RemoteFile, adapter: Adapter, target: Path) -> bool:
    """Fallback for servers that refuse range requests."""
    print("  server does not support ranges - falling back to one connection")
    live = sys.stdout.isatty()  # don't spam a redirected log with progress lines
    session = make_session(adapter.ip)
    try:
        response = session.get(remote.url, stream=True, allow_redirects=True, timeout=(10, 30))
        response.raise_for_status()
        done = 0
        started = time.monotonic()
        with open(target, "wb") as handle:
            for block in response.iter_content(BLOCK):
                handle.write(block)
                done += len(block)
                if live:
                    rate = done / max(time.monotonic() - started, 1e-6)
                    sys.stdout.write(f"\r  {human_bytes(done)}  {human_bytes(rate)}/s   ")
                    sys.stdout.flush()
        if live:
            print()
        print(f"  got {human_bytes(done)}")
        return True
    finally:
        session.close()


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Download one file over several network connections at once.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("url", nargs="?", help="direct HTTP/HTTPS link")
    parser.add_argument("-o", "--output", help="output file or folder")
    parser.add_argument("--list", action="store_true", help="show network adapters and exit")
    parser.add_argument("--only", help='use only these adapters, comma separated')
    parser.add_argument("--exclude", help="skip these adapters, comma separated")
    parser.add_argument("--limit", action="append", default=[],
                        help='cap one adapter, e.g. --limit "iPhone USB=500KB"')
    parser.add_argument("--connections", type=int, default=4,
                        help="connections per adapter (default 4)")
    args = parser.parse_args()

    adapters = discover_adapters()

    if args.list:
        if not adapters:
            print("No usable network adapters found.")
            return 1
        width = max(len(a.name) for a in adapters)
        print("Network adapters:")
        for adapter in adapters:
            print(f"  {adapter.name:<{width}}  {adapter.ip}")
        return 0

    if not args.url:
        parser.error("give a URL, or use --list")

    if not adapters:
        print("No usable network adapters found.")
        return 1

    # filter by name
    if args.only:
        wanted = {n.strip().lower() for n in args.only.split(",")}
        adapters = [a for a in adapters if a.name.lower() in wanted]
    if args.exclude:
        unwanted = {n.strip().lower() for n in args.exclude.split(",")}
        adapters = [a for a in adapters if a.name.lower() not in unwanted]
    if not adapters:
        print("No adapters left after --only/--exclude.")
        return 1

    # per adapter speed caps
    for entry in args.limit:
        if "=" not in entry:
            print(f"Ignoring --limit {entry!r} (expected NAME=SIZE)")
            continue
        name, value = entry.split("=", 1)
        try:
            rate = parse_size(value)
        except argparse.ArgumentTypeError as error:
            print(f"Bad --limit value: {error}")
            return 1
        if not any(a.name.lower() == name.strip().lower() for a in adapters):
            print(f"Ignoring --limit for {name.strip()!r} - no such adapter in use")
            continue
        for adapter in adapters:
            if adapter.name.lower() == name.strip().lower():
                adapter.limiter = RateLimiter(rate)

    print(f"Resolving {args.url}")
    try:
        remote = probe_remote(make_session(None), args.url)
    except Exception as error:
        print(f"Could not reach that URL: {error}")
        return 1

    # where to write
    if args.output:
        out = Path(args.output).expanduser()
        target = out / remote.filename if out.is_dir() else out
    else:
        target = Path.cwd() / remote.filename
    target.parent.mkdir(parents=True, exist_ok=True)

    size_text = human_bytes(remote.size) if remote.size else "unknown size"
    print(f"  {remote.filename}  ({size_text})")

    print("Testing networks...")
    adapters = usable_adapters(adapters, remote.url)
    if not adapters:
        print("None of your networks could reach that URL.")
        return 1
    for adapter in adapters:
        print(f"  using {adapter.name} ({adapter.ip})")

    if not remote.supports_ranges or not remote.size:
        return 0 if single_stream(remote, adapters[0], target) else 1

    # Chunk sizing depends only on the file, never on how many adapters or
    # connections this run happens to use - otherwise changing --connections
    # between runs would invalidate the resume state and restart from zero.
    chunk_size = max(MIN_CHUNK, min(MAX_CHUNK, remote.size // TARGET_CHUNKS or MIN_CHUNK))

    state = State(target.with_suffix(target.suffix + ".mpstate"))
    resuming = state.load(remote.url, remote.size, chunk_size, remote.etag) and target.exists()
    if resuming:
        print(f"  resuming - {len(state.done)} chunks already on disk")
    else:
        state.done.clear()
        state.start(remote.url, remote.size, chunk_size, remote.etag)
        with open(target, "wb") as handle:  # preallocate so workers can seek
            handle.truncate(remote.size)

    fancy = enable_ansi()
    print()
    download = Download(remote, adapters, target, args.connections, chunk_size, state)
    completed = download.run(fancy)

    if not completed:
        if download.errors:
            print("\nProblems:")
            for message in dict.fromkeys(download.errors):
                print(f"  {message}")
        return 1

    state.clear()
    elapsed = time.monotonic() - download.started
    average = remote.size / max(elapsed, 1e-6)
    print(f"\nDone in {human_time(elapsed)}  (avg {human_bytes(average)}/s)")
    print(f"Saved to {target}")
    for adapter in adapters:
        share = adapter.downloaded / remote.size * 100 if remote.size else 0
        print(f"  {adapter.name}: {human_bytes(adapter.downloaded)} ({share:.0f}%)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print()
        sys.exit(130)
