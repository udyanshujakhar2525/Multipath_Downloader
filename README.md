# multipath

A download manager that pulls one file over several internet connections at once.

Most download managers open several connections to a server, but they all leave
your computer through one network. multipath binds each connection to a specific
network adapter, so an Ethernet cable, a Wi-Fi network and a phone tethered over
USB can all fetch parts of the same file in parallel. Faster links take a bigger
share of the work automatically.

![The multipath UI](multipath-ui.png)

## Requirements

- Python 3.9 or newer
- `pip install requests psutil`

Built and tested on Windows. The source-IP binding it relies on also works on
macOS and Linux, but on Linux the default route can override it unless you add
policy routing — see [Platform notes](#platform-notes).

## Install

Put `multipath.py` and `multipath_ui.py` in the same folder, then:

```
pip install requests psutil
```

## The interface

```
python multipath_ui.py
```

This starts a small local server and opens the page in your browser. Downloads go
to your Downloads folder unless you change **Save to**.

The graph is the main readout: one coloured band per network, stacked so the
bands add up to your real combined speed. If a link is not pulling its weight,
its band is thin. Each network also gets a switch and an optional speed cap, and
every file shows a bar underneath it breaking down which network supplied what.

| Option | What it does |
| --- | --- |
| `--port 8765` | Pick a port. Falls back to a free one if taken. |
| `--folder PATH` | Where to save files. |
| `--connections N` | Connections per network, 1–16 (default 4). |
| `--no-browser` | Don't open a browser window. |

Settings and per-network caps are remembered in `~/.multipath-ui.json`.

The server binds to `127.0.0.1` only, so it is not reachable from your network.
The page loads no webfonts or external scripts, so it looks the same offline and
makes no outbound request when you open it.

## The command line

The engine also runs on its own, with no server and no browser:

```
python multipath.py --list                       show your network adapters
python multipath.py <url>                        download using every usable network
python multipath.py <url> -o C:\Downloads\x.iso  choose the output path
python multipath.py <url> --only "Ethernet,Wi-Fi"
python multipath.py <url> --exclude "iPhone USB"
python multipath.py <url> --limit "iPhone USB=500KB"
python multipath.py <url> --connections 6
```

Start with `--list`. If your adapters appear with separate IP addresses, binding
is working.

Press Ctrl+C to stop; run the same command again to resume.

## How it works

The file is split into fixed-size chunks held in one shared queue. Worker threads
pull a chunk, request exactly that byte range over HTTP, and write it straight
into the right offset of a preallocated file. Because workers take a new chunk
only when they finish the last one, a fast link naturally completes more chunks
than a slow one — there is no bandwidth measuring or manual tuning involved.

Each worker's socket is pinned to one adapter's local IP address via a custom
`requests` transport adapter. Proxy environment variables are deliberately
ignored: a proxy would funnel every connection out of one route and silently
defeat the whole thing.

Chunk size depends only on the size of the file, never on how many networks or
connections a particular run uses, so changing `--connections` between runs does
not invalidate a partly finished download.

**Resuming.** Completed chunk indexes are written to a `.mpstate` file next to
the download. On restart, that state is reused only if the file size, chunk size
and `ETag` all still match, so a file that changed on the server is downloaded
again rather than being stitched together from two different versions. The state
file is deleted once the download finishes.

**Rate limits** are token buckets, one per network, starting empty rather than
full so the first second does not run over the cap.

**Servers that refuse range requests** fall back to a single plain connection on
one network. The download still works, just without the speedup.

## Platform notes

Binding a socket to an adapter behaves differently per OS:

- **Windows** uses the weak host model, so binding to a source IP routes out of
  the matching adapter. This is the case multipath was built for.
- **macOS** generally honours source-IP binding for this purpose.
- **Linux** may send packets out of the default route regardless of source IP.
  Getting true per-adapter behaviour usually needs `SO_BINDTODEVICE` (which
  requires root) or per-adapter policy routing (`ip rule` / `ip route`).

## Limitations

- **No torrents.** Magnet links and `.torrent` files are not supported; that
  needs a full BitTorrent stack.
- **One file at a time.** Extra downloads queue. This is deliberate: every link
  on one file beats splitting your links across two.
- **Shared upstream links don't help.** If your tethered phone and your Wi-Fi
  reach the internet through the same connection, you will see both bands in the
  graph but the total will not rise. They are separate interfaces, not separate
  pipes. The graph makes this visible rather than hiding it.
- **Switching a network off mid-download restarts that download.** A running
  download holds a fixed set of adapters, so the switch can only take effect by
  restarting it. Finished chunks are kept, so this costs about a second.
- Direct HTTP/HTTPS links only. Pages that require a login, or that hand out
  links through a browser session, won't work.

## Testing

The engine and UI were verified against a local range-capable HTTP server, with
SHA-256 compared against the source on every run:

- a full download, byte-for-byte identical
- interrupt and resume, reconstructing an identical file, including across a
  changed `--connections` value
- the no-range single-connection fallback
- rate limiters measuring 100% of their configured cap at several rates and
  thread counts
- work-stealing balance: with caps 10x apart, the fast link took 7–9x more of
  the file
- error handling for bad links, bad `--limit` values and empty adapter filters
- every UI interaction driven in a real browser — add, pause, resume, toggle a
  network, add while running, reject a malformed link — ending with matching
  checksums, no console errors and no overflow at phone width
- per-network attribution reconciling to the byte across toggles and pauses
  (181 + 50 + 19 = 250.0 MB)

Multi-adapter balancing was verified with several rate-limited adapters over one
physical interface. True multi-interface binding can only be confirmed on a
machine that has more than one link; `--list` is the quick check.

## Files

| File | |
| --- | --- |
| `multipath.py` | The download engine, and a complete CLI. Works on its own. |
| `multipath_ui.py` | The local web UI. Needs `multipath.py` beside it. |
