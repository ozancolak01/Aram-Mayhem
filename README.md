# League of Legends — ARAM: Mayhem (Multiplayer & Solo)

A real-time 16-champion Howling Abyss ARAM game with Wild Rift mobile touch controls, tactical pings, custom bots (0 by default), item shop, and mayhem augments.

## Deploying to Render (Free with GitHub)
1. Create a new repository on GitHub (e.g. `aram-mayhem`).
2. Upload this folder contents (`server.js`, `package.json`, `render.yaml`, and `public/index.html`).
3. On Render (dashboard.render.com), click **New +** -> **Web Service**.
4. Connect your GitHub repository.
5. Settings:
   - **Runtime**: `Node`
   - **Build Command**: *(leave empty)*
   - **Start Command**: `node server.js`
   - **Instance Type**: `Free`
6. Click **Deploy Web Service**! Render will give you a public `https://<your-app>.onrender.com` URL that you and your friends can play on from mobile or desktop browsers anywhere in the world.
