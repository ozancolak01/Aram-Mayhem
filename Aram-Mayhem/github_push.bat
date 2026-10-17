@echo off
chcp 65001 >nul
echo ===================================================
echo   ARAM: MAYHEM - GITHUB TEK TIKLA GUNCELLEME
echo   Repo: https://github.com/ozancolak01/Aram-Mayhem.git
echo ===================================================
if not exist ".git" (
    git init
    git branch -M main
    git remote add origin https://github.com/ozancolak01/Aram-Mayhem.git
)
git add index.html public/index.html server.js package.json render.yaml README.md .gitignore github_push.bat
git commit -m "Update ARAM: Mayhem v8 (24 Champions, Evolution & Balance Overhaul)"
git push -u origin main --force
echo.
echo [TAMAMLANDI] Dosyalar GitHub reponuza basariyla gonderildi!
pause
