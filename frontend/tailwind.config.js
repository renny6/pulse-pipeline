/** @type {import('tailwindcss').Config} */
// ===========================================================================
// PULSE DASHBOARD — Tailwind v4 Config Reference
//
// NOTE: Tailwind v4 is CSS-first. The authoritative design tokens are defined
// in src/index.css under the @theme block, NOT here. This file exists only as
// a documented reference for the color/typography contract and for tooling
// compatibility (IDE autocomplete, class detection, etc.).
//
// Color Palette:
//   black          #000000   True Black — app background
//   charcoal       #0A0A0A   Deep Charcoal — shell background
//   card           #111111   Panel / card surfaces
//   border         #222222   Default dividers
//   neon-green     #00FF41   Healthy / live status (Matrix Green)
//   crimson        #FF003C   Warning / error / alert (Crimson Red)
//   electric-blue  #00E5FF   Info / active state (Electric Blue)
//
// Typography:
//   sans   Inter (400/500/600)
//   mono   JetBrains Mono (400/500/600) — tabular-nums for metric stability
//
// These map to Tailwind utility classes via the @theme block in index.css:
//   bg-neon-green, text-crimson, border-electric-blue, font-mono, etc.
// ===========================================================================
export default {
  content: [
    "./index.html",
    "./src/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Exact hex values mirror the @theme CSS variables
        black:          "#000000",
        charcoal:       "#0A0A0A",
        card:           "#111111",
        border:         "#222222",
        "border-subtle":"#1A1A1A",
        "neon-green":   "#00FF41",
        crimson:        "#FF003C",
        "electric-blue":"#00E5FF",
        "text-primary": "#E8E8E8",
        "text-secondary":"#666666",
        "text-muted":   "#3A3A3A",
      },
      fontFamily: {
        mono: [
          "'JetBrains Mono'", "'Fira Code'", "'Cascadia Code'",
          "ui-monospace", "'SF Mono'", "Menlo", "Consolas", "monospace",
        ],
        sans: [
          "'Inter'", "'Segoe UI'", "system-ui", "-apple-system", "sans-serif",
        ],
      },
    },
  },
  plugins: [],
}
