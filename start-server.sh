#!/usr/bin/env bash
# ARAM: Mayhem - start the game server on this machine (macOS / Linux).
cd "$(dirname "$0")"
command -v node >/dev/null || { echo "Node.js 18+ is required: https://nodejs.org"; exit 1; }
if [ ! -f public/tailwind.css ]; then
  echo "Building styles once (needs internet, optional)..."
  npm install --no-audit --no-fund >/dev/null 2>&1 && npm run build:css >/dev/null 2>&1
fi
echo "Starting server... Ctrl+C to stop. For friends outside your network run ./start-tunnel.sh in a second terminal."
exec node server.js
