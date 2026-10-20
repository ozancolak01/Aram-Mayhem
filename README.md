# ARAM: Mayhem

A League-of-Legends-style **ARAM: Mayhem** that runs in the browser, with a small zero-dependency
Node.js server for multiplayer lobbies. Play solo against bots, or create a room and play with friends.

## Quick start

**Windows:** double-click `start-server.bat`. **macOS / Linux:** `./start-server.sh`. Or by hand:

```bash
npm install          # only needed for the Tailwind build; the server itself has no runtime dependencies
npm run build:css    # compiles public/tailwind.css (optional: the page falls back to the Tailwind CDN without it)
npm start            # http://localhost:3000
```

The server prints a LAN address too (e.g. `http://192.168.1.20:3000`) so phones and other PCs on the same
network can join. Node.js 18+ is required. Set `PORT` to change the port.

### Playing with friends who are not on your network

Run a free Cloudflare quick tunnel next to the server: `start-tunnel.bat` / `./start-tunnel.sh` (install
`cloudflared` once: `winget install --id Cloudflare.cloudflared` on Windows). It prints an
`https://….trycloudflare.com` link; friends open it in their browser (the game switches to `wss://` automatically).
The link changes every time you start the tunnel. Your PC must stay on while you play.

Tip: the host's browser runs the match, so the best host is the player with the strongest PC and connection, and
that player should also run the server (their own connection to it is then local).

### Playing on phones

Open the same link on the phone (the LAN address or the tunnel link). The game detects touch devices and switches
to its mobile layout with a joystick and on-screen buttons; turn the phone to **landscape** (the game asks for
fullscreen + landscape on first touch).

* **Best setup: a PC is the host.** The host's device runs the whole match, which is heavy for a phone and stops
  the moment the phone's screen locks or the browser goes to the background. Phones are great as players.
* **If the host's snapshots stop for 5 s** (phone locked, tab suspended) the server automatically moves the host role
  to another player, preferring a desktop, and the match continues. The old host rejoins as a normal player.
* **Screen stays on** during play on `https` links (the tunnel link) via the Screen Wake Lock API. On plain
  `http://192.168…` LAN addresses browsers do not allow wake locks, so turn off auto-lock for the session or use the tunnel link.
* **Dead connections are detected** (Wi-Fi to mobile data, locked screen): the match freezes, the phone reconnects by
  itself, and when you unlock the phone it reconnects immediately.
* Add the page to the home screen (Share / menu → *Add to Home Screen*) for a browser-bar-free fullscreen view.

## How to play

1. Enter a name and **create a room** (`1v1`, `3v3` or `5v5` preset) or join one from the lobby browser.
2. The host chooses bot difficulty (**Easy / Normal / Hard**) and fills or clears bot slots. Bots fill every
   empty seat, and a human can take over a bot at any time, even in a running match (or spectate).
3. **Champion select** lasts 30 s (it shortens to 3 s once everybody has locked in). You get 2 rerolls and a
   shared team bench you can swap with. Pick two summoner spells (Snowball, Flash, Ghost, Ignite, Heal,
   Barrier, Clarity).
4. **Match:** one lane, minion waves every ~21 s, turrets, inhibitors and a Nexus per team. Destroy the enemy
   **Nexus** to win. Gold and XP trickle in automatically (ARAM style), and at levels **7 / 11 / 15** you pick
   a **Mayhem augment**. There are items to buy at the fountain shop, relics, hexgates and roaming poros.

### Controls

| Action | Classic (LoL) | WASD scheme | Mobile |
|---|---|---|---|
| Move / attack | Right-click (left-click also commands) | same | virtual joystick + buttons |
| Abilities | `Q` `W` `E` `R` | `1`/`Q`, `2`, `3`/`E`, `4`/`R` | on-screen buttons |
| Summoner spells | `D` / `F` | `G` (D slot) / `F` | on-screen buttons |
| Attack-move / Stop | `A` / `S` | – | – |
| Frostgate (hexgate) / Poro Snax | `G` / `4` | – | on-screen buttons |
| Shop | `P` or `B` | `P` or `B` | button |
| Scoreboard | `Tab` | `Tab` | button |
| Lock/free camera | `Y` (middle-mouse drag pans) | same | button |
| Zoom | mouse wheel | same | – |
| Chat | `Enter` | same | – |
| Pings | ping buttons, then click map/minimap | same | same |

Pick the control scheme in the lobby settings.

## Architecture

```
public/index.html   the whole client (game, rendering, lobby UI), served as a static file
server.js           HTTP static server + hand-written RFC 6455 WebSocket server + lobby/room logic
```

**The host's browser runs the simulation.** The server does not simulate the game; it is a room manager
and a relay:

* The room's **host** simulates minions, turrets, bots and combat, and streams a full world snapshot ~20x/s.
* Every other player simulates only their own champion locally, sends their inputs (`player_action`,
  `player_state`), and renders the host's snapshots.
* If the host leaves, the server migrates the host role to another player, who continues from the latest snapshot.

### Connection handling (what keeps matches in sync)

* **Latest-wins snapshots.** The server forwards the host's snapshot text untouched and never queues stale ones: if a
  client's connection is slower than the stream, older snapshots are dropped and the client gets the newest one as
  soon as its socket drains. Only the current host can publish world state.
* **Reconnect grace.** A player who disconnects keeps their champion for 25 s. If they come back (same browser
  profile/token) they resume the match from the latest snapshot; otherwise a bot takes over. While offline, the
  client **freezes** the match instead of simulating a private copy.
* **Rooms outlive blips.** A room with nobody connected is kept for 15 s (lobby), 30 s (champion select) or
  120 s (running match) before it is deleted.
* **Background tabs.** On desktop the host's simulation keeps running (via a Web Worker timer) when the tab is hidden,
  so alt-tabbing does not freeze everyone else. Phones suspend background browsers, which is why the server can
  hand the host role to another player (see *Playing on phones*).
* **Hardened WebSocket layer:** 512 KB message limit (checked before buffering), fragmentation support, masked-frame
  validation, proper ping/pong/close handshake, server heartbeat every 15 s (keeps Fly/Render proxies from idling the
  connection out), per-client rate limiting and a 500-connection cap.

## Diagnostics

* `GET /stats` (local only; behind a tunnel set `STATS_KEY=secret` and open `/stats?key=secret`) lists rooms, who is
  host, and for every player how stale their view of the match is (`snapshotAgeMs`) and whether the host's tab is hidden.
* The server logs a `[sync]` warning whenever a non-host has not applied a host snapshot for 2.5 s or more, with
  the likely culprit (their socket backlog, or a hidden host tab).
* Player names, room names and chat are stripped of `<` `>` and escaped again by the client before display.

## Static files

Everything in `public/` is served (with gzip, ETag and path-traversal protection). Add images or sounds there and
reference them as `/my-file.png`. `GET /healthz` returns `ok` for health checks.

## Deploying (optional, instead of running on your PC)

State (rooms, running matches) lives in the memory of a single server process, so **run exactly one instance**.
Free always-on hosting for a running server mostly no longer exists; a cheap VPS or your own PC plus a tunnel are the
reliable options.

**Fly.io**
```bash
fly deploy
fly scale count 1      # fly may create 2 machines by default; one is what you want
```
The provided `fly.toml` keeps the machine running (`auto_stop_machines = 'off'`) so rooms are not lost to sleep.

**Render**: use `render.yaml`. The free plan sleeps when idle and drops all rooms; use a paid plan for always-on.
**Railway / Heroku / any Node host:** `npm install && npm run build:css`, then `npm start`.

## Troubleshooting

* *Other players freeze or see a different match:* make sure only one server instance is running, and that everybody
  loaded the same deployed version (hard-refresh). The in-game badge shows `HOST • ms` or `SYNC • ms`; a high
  `SYNC` ping points at a slow connection (or a slow host), not at the server.
* *Page looks unstyled:* run `npm run build:css` (without it the page loads Tailwind from the CDN, which needs internet).
