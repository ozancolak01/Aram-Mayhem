@echo off
REM ARAM: Mayhem - start the game server on this PC (Windows).
REM Double-click this file. Friends on the same network open the LAN address it prints.
cd /d "%~dp0"
where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed. Get the LTS version from https://nodejs.org and run this again.
  pause
  exit /b 1
)
if not exist public\tailwind.css (
  echo Building styles once ^(needs internet, optional^)...
  call npm install --no-audit --no-fund >nul 2>nul
  call npm run build:css >nul 2>nul
)
echo.
echo Starting server... keep this window open while you play.
echo For friends outside your network, run start-tunnel.bat in a second window.
echo.
node server.js
pause
