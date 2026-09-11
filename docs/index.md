# Relay runbook

Operating notes for this Telegram **WEB proxy** relay. It puts one hostname in
front of a stock MTProxy and carries traffic over ordinary HTTPS/WebSocket from
the client's WebView, so the hostname looks like any other website.

The design is in [README.md](https://github.com/retro1878/tproxy-server/blob/master/README.md)
and the specs ([PROTOCOL.md](https://github.com/retro1878/tproxy-server/blob/master/PROTOCOL.md),
[BASE_PATH.md](https://github.com/retro1878/tproxy-server/blob/master/BASE_PATH.md),
[PUBLIC_SITE.md](https://github.com/retro1878/tproxy-server/blob/master/PUBLIC_SITE.md),
[ANDROID.md](https://github.com/retro1878/tproxy-server/blob/master/ANDROID.md)).
This page is only what you need while running it.

**Two things to internalise first.**

1. **The relay reads its configuration once, at startup, and never reloads.**
   Editing `config.json` or `profiles.json` changes nothing until
   `systemctl restart tproxy-server`. This is the single most common cause of a
   link that "should work" and doesn't — including the link the installer just
   printed.
2. **The hostname is the whole public surface.** Port 2398 must stay
   loopback-only. Anything that exposes it defeats the point.

## Layout

```
client WebView
   -> Caddy :80/:443            (TLS, ACME)
   -> relay 127.0.0.1:8080      (bridge, carrier, public site)
   -> MTProxy 127.0.0.1:2398
```

Admin and health are on `127.0.0.1:8081` (`/healthz`, `/readyz`, `/metrics`).
`/readyz` dials every profile's backend, so it fails whenever MTProxy is down.
MTProxy's own counters are on `127.0.0.1:8888/stats`.

## Install

```bash
sudo ./deploy/install.sh --hostname proxy.example.com --email you@example.com --site-dir ~/my-site
```

`--site-dir` (or `--site-upstream`) is required on a fresh host; later runs reuse
`/srv/tproxy-site`. Add `--secret random` to run unattended. The installer prints
the client link at the end; `sudo ./deploy/show-link.sh` reprints it later.

Re-running the installer is safe. It preserves the secret, the base path, the
promo tag and the carrier mode, and restarts every unit, so a change made by hand
is not silently reverted.

## Verify

```bash
curl --fail http://127.0.0.1:8081/readyz                                    # backend reachable
curl -s http://127.0.0.1:8888/stats | grep -E "proxy_tag_set|proxy_mode"    # 1 and 2
curl --fail https://proxy.example.com/                                      # public site
```

`proxy_mode 2` is `PROXY_MODE_OUT`, the only normal value. A link that
authenticates returns the bridge document instead of the public site.

## Hand out the link

```text
tg://webproxy?server=proxy.example.com[/base_path]&secret=<client secret>
```

Prefer `tg://` — the public `t.me/webproxy` frontend does not register that path.
The installer prints two secrets and they are not interchangeable: the **internal
secret** is what MTProxy is configured with, the **client secret** is what goes in
the link. Under a base path the client secret is an encoding of the internal one,
never the internal value itself.

## Changing anything

| What | Where | Then |
|---|---|---|
| hostname, base path, static routes, limits | `/etc/tproxy-server/config.json` | `systemctl restart tproxy-server` |
| secret, backend, carrier mode | `/etc/tproxy-server/profiles.json` | `systemctl restart tproxy-server` |
| public site files | `/srv/tproxy-site` | `systemctl restart tproxy-server` |
| promo tag, NAT args, workers | `/etc/mtproxy/mtproxy.env` | `systemctl restart mtproxy` |
| TLS, headers, timeouts | `/etc/caddy/Caddyfile` | `systemctl reload caddy` |

Restarting the relay drops live carrier sessions; clients reload the bridge and
reconnect. Do it at a quiet time if people are connected.

`profiles.json` must stay `0400 root:tproxy`. The loader rejects a file that is
readable or writable by group or others, so a plain `sed -i` that leaves it `0644`
stops the relay from starting.

## Promoted channel

`--mtproxy-tag <32 hex>` sets the promo tag. The tag alone promotes nothing —
the channel must also be bound to the proxy in [@MTProxybot](https://t.me/MTProxybot)
→ `/myproxies`. Expect a delay after binding; it can take a while and needs no
further action.

```bash
curl -s http://127.0.0.1:8888/stats | grep -E "proxy_tag_set|tot_forwarded_queries"
```

`proxy_tag_set 1` with `tot_forwarded_queries` rising means the server half is
correct and nothing left to fix locally. The tag travels proxy-to-Telegram, so it
survives a client-secret rotation and needs no re-registration.

## Carrier modes

| Mode | Shape | Pick it when |
|---|---|---|
| `https` (default) | one serialized POST plus one long poll | baseline |
| `https-lanes` | a lane per logical Telegram stream | you want latency isolation; needs HTTP/2 |
| `websocket` | one WebSocket multiplexing everything | you want to drop HTTP stop-and-wait |
| `websocket-lanes` | a WebSocket per logical stream | bulk media must not block interactive traffic |

```bash
sudo ./deploy/install.sh --hostname proxy.example.com --email you@example.com --carrier-mode websocket
```

The mode is baked into the bridge document, so the client must reload the bridge
(reconnect the proxy) before it applies. macOS WebKit serializes WebSocket
handshakes, so `websocket-lanes` can time out there — use `websocket` instead.

## Behind a CDN

```bash
sudo ./deploy/install.sh --hostname proxy.example.com --email you@example.com --behind-cdn
```

The flag pins the per-IP limits to 0 — behind an edge they would count the edge,
not the user — and then checks the two settings that break the carrier silently:
a cookie on an API request, and an `X-Forwarded-For` that is not exactly one IP.
Both make the relay answer **404**, with nothing in any log.

In the CDN dashboard: SSL/TLS **Full (strict)**, and everything that rewrites HTML
or injects JavaScript turned **off**. The bridge is served under a nonce CSP with
an inline script, so any injection either breaks it or is blocked by it.

Leave `trusted_proxies` unset in the Caddyfile. Marking the CDN trusted makes
Caddy append rather than replace the forwarding header, and the relay then 404s
the bridge and API while `/readyz` stays green.

Confirm certificate renewal. TLS-ALPN-01 cannot complete through a proxying CDN,
so Caddy has to fall back to HTTP-01 on port 80.

## Troubleshooting

**The link does not work at all, but the relay looks healthy.**
Restart the relay and retest before theorising. A running relay can be serving
the hostname, base path or secret of an earlier install, and every health check
will still pass. Do not trust the printed link — trust the running process.

**The website loads but the proxy does not.**
Localise before changing anything. Snapshot, attempt a connection, snapshot
again:

```bash
sudo curl -s http://127.0.0.1:8081/metrics | grep -E "sessions_created_total|streams_opened_total|bytes_up_total|backend_dial_failures_total"
sudo curl -s http://127.0.0.1:8888/stats | grep -E "active_connections|tot_forwarded_queries"
```

Nothing moves: the attempt never reached the relay, so the problem is the client
or the path to it, and no relay setting will help. Relay counters move but
MTProxy's do not: the failure is inside the relay. Both move: the traffic reached
Telegram.

**An unexplained 404 on the bridge or the API.**
Check for a cookie on the request and for an `X-Forwarded-For` carrying more than
one address. Both are refused by design, and both look like a dead proxy rather
than an error.

**A client that "does not support this".**
Test the deployment with the working reference before concluding the client is at
fault. Clients do support this protocol; a failed link usually means the running
relay does not hold the configuration on disk.

## What this fork adds

Fixes for a fresh `deploy/install.sh` that cannot complete upstream (`umask 077`
breaking the test run and the MTProxy build), an installer that never restarted
the relay, and an inert `-p 8888` stats port. Plus `--mtproxy-tag`,
`--carrier-mode` and `--behind-cdn`. See the commit history for the reasoning.
