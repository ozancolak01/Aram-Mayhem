@echo off
REM Gives your local server a public https link via a free Cloudflare quick tunnel.
REM Install cloudflared once:  winget install --id Cloudflare.cloudflared
where cloudflared >nul 2>nul
if errorlevel 1 (
  echo cloudflared is not installed. Run this once, then try again:
  echo    winget install --id Cloudflare.cloudflared
  pause
  exit /b 1
)
echo Start start-server.bat first. Look for the https://....trycloudflare.com link below and send it to your friends.
cloudflared tunnel --url http://localhost:3000
pause
