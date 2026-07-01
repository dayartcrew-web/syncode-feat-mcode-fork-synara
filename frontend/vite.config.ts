import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

const host = process.env.TAURI_DEV_HOST;

export default defineConfig(async () => ({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
      // Path-identical shim: `@t3tools/contracts` resolves to the local bridge
      // package so a cloned MCode UI keeps its imports verbatim.
      // See docs/CONTRACTS-BRIDGE-DESIGN.md §3.1.
      "@t3tools/contracts": path.resolve(__dirname, "./src/contracts"),
    },
  },
  clearScreen: false,
  server: {
    port: 5173,
    strictPort: true,
    host: host || false,
    hmr: host
      ? { protocol: "ws", host, port: 5174 }
      : undefined,
    watch: {
      ignored: ["**/src-tauri/**"],
    },
  },
}));
