/** Tailwind is compiled at build time (npm run build:css) instead of using the runtime "Play CDN". */
module.exports = {
  content: ['./public/index.html'],
  theme: { extend: {} },
  plugins: []
};
