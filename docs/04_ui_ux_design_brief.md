# 04 | UI/UX Design Brief

## Aesthetic & Vibe
* **Style:** Tactical, cyber-physical control center. Highly technical but incredibly clean.
* **Vibe References:** Vercel Dashboard, Linear, Datadog.
* **Mode:** Strict Dark Mode exclusively. No light mode support.

## Color Palette
* **Background:** True Black (`#000000`) or deep charcoal (`#0A0A0A`).
* **UI Panels/Cards:** Very dark gray (`#111111`) with subtle `1px` borders (`#222222`).
* **Text:** Primary (`#EDEDED`), Secondary/Muted (`#888888`).
* **Status Colors (Crucial for Animations):**
  * **Success/Pass:** Neon Matrix Green (`#00FF41`).
  * **Blocked/Throttled:** Crimson Warning Red (`#FF003C`).
  * **Processing/Kafka:** Electric Blue (`#00E5FF`).

## Typography
* **UI & Headings:** `Inter` (sans-serif, clean, tight tracking).
* **Metrics, Numbers & Logs:** `Geist Mono` or `JetBrains Mono` (tabular numbers ensure live-updating metrics don't "jiggle" horizontally).

## Component Styling
* **Corners:** Sharp or slightly rounded (`border-radius: 4px`). 
* **Shadows:** No heavy drop shadows. Use subtle glows (box-shadow using the status colors) to indicate active/hot nodes.

## Animations
* Particle streams on the canvas must be fluid (60fps). React state updates must NOT interfere with the canvas rendering loop.
