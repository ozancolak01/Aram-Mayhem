# syntax = docker/dockerfile:1
ARG NODE_VERSION=22.21.1

# ── Build stage: compile Tailwind (dev dependency) ──────────────────────────
FROM node:${NODE_VERSION}-slim AS build
WORKDIR /app
COPY package.json ./
RUN npm install
COPY tailwind.config.js ./
COPY src ./src
COPY public ./public
RUN npm run build:css

# ── Runtime stage: the server has zero runtime dependencies ─────────────────
FROM node:${NODE_VERSION}-slim
WORKDIR /app
ENV NODE_ENV=production
COPY package.json server.js ./
COPY --from=build /app/public ./public
EXPOSE 3000
USER node
CMD ["node", "server.js"]
