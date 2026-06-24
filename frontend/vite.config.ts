import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vite.dev/config/
// Tailwind v4 uses the Vite plugin (not PostCSS) for zero-config integration.
export default defineConfig({
  plugins: [
    tailwindcss(),
    react(),
  ],
})
