#!/usr/bin/env bash
# Public https link to your local server (free Cloudflare quick tunnel). Install: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/
command -v cloudflared >/dev/null || { echo "Install cloudflared first (see link in this file)."; exit 1; }
echo "Start the server first (./start-server.sh). Send the https://....trycloudflare.com link to your friends."
exec cloudflared tunnel --url http://localhost:3000
