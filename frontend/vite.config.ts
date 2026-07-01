// FILE: vite.config.ts
// Purpose: Builds the Syncode-vendored MCode web client.
// Layer: Web build config (merged from MCode apps/web + T1 Tauri aliases).
//
// This config merges MCode's Vite 8 build (Tailwind v4, TanStack Router
// plugin, React compiler, central-icon-prune plugin) with T1's path-identical
// `@t3tools/contracts` / `@t3tools/shared` shim aliases and Tauri dev-server
// settings. See docs/CONTRACTS-BRIDGE-DESIGN.md §3.1 for the shim strategy.

import fs from "node:fs/promises";
import path from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import babel from "@rolldown/plugin-babel";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import { defineConfig, type Plugin } from "vite";

const host = process.env.TAURI_DEV_HOST;
const port = Number(process.env.PORT ?? 5173);
const sourcemapEnv = process.env.MCODE_WEB_SOURCEMAP?.trim().toLowerCase();

const buildSourcemap =
  sourcemapEnv === "1" || sourcemapEnv === "true"
    ? true
    : sourcemapEnv === "hidden"
      ? "hidden"
      : false;

const CENTRAL_ICON_DIR = "central-icons-reversed";
const CENTRAL_ICON_NAME_PATTERN = /^[a-z0-9][a-z0-9-]*$/;
const SOURCE_EXTENSIONS = new Set([".ts", ".tsx", ".js", ".jsx"]);

async function listFiles(root: string): Promise<string[]> {
  const entries = await fs.readdir(root, { withFileTypes: true }).catch(() => []);
  const result: string[] = [];
  for (const entry of entries) {
    const entryPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      result.push(...(await listFiles(entryPath)));
    } else if (entry.isFile()) {
      result.push(entryPath);
    }
  }
  return result;
}

// Finds literal icon basenames in source, then prunes the copied public icon set after build.
function centralIconPrunePlugin(): Plugin {
  let resolvedRoot = process.cwd();
  let resolvedOutDir = "dist";
  return {
    name: "mcode-central-icon-prune",
    apply: "build",
    configResolved(config) {
      resolvedRoot = config.root;
      resolvedOutDir = path.resolve(config.root, config.build.outDir);
    },
    async closeBundle() {
      const publicIconDir = path.join(resolvedRoot, "public", CENTRAL_ICON_DIR);
      const distIconDir = path.join(resolvedOutDir, CENTRAL_ICON_DIR);
      const iconFiles = await fs.readdir(publicIconDir).catch(() => []);
      const availableIcons = new Set(
        iconFiles
          .filter((name) => name.endsWith(".svg"))
          .map((name) => name.slice(0, -".svg".length)),
      );
      if (availableIcons.size === 0) return;

      const sourceFiles = (await listFiles(path.join(resolvedRoot, "src"))).filter((file) =>
        SOURCE_EXTENSIONS.has(path.extname(file)),
      );
      const requiredIcons = new Set<string>();
      const literalPattern = /["'`]([a-z0-9][a-z0-9-]*)["'`]/g;
      for (const sourceFile of sourceFiles) {
        const source = await fs.readFile(sourceFile, "utf8").catch(() => "");
        for (const match of source.matchAll(literalPattern)) {
          const iconName = match[1];
          if (
            iconName &&
            CENTRAL_ICON_NAME_PATTERN.test(iconName) &&
            availableIcons.has(iconName)
          ) {
            requiredIcons.add(iconName);
          }
        }
      }

      if (requiredIcons.size === 0) return;
      const copiedIconFiles = await fs.readdir(distIconDir).catch(() => []);
      let removedCount = 0;
      await Promise.all(
        copiedIconFiles.map(async (fileName) => {
          if (!fileName.endsWith(".svg")) return;
          const iconName = fileName.slice(0, -".svg".length);
          if (requiredIcons.has(iconName)) return;
          removedCount += 1;
          await fs.rm(path.join(distIconDir, fileName), { force: true });
        }),
      );
      console.info(
        `[central-icons] kept ${requiredIcons.size}/${availableIcons.size} referenced SVGs, pruned ${removedCount}.`,
      );
    },
  };
}

export default defineConfig({
  plugins: [
    tanstackRouter({
      target: "react",
      autoCodeSplitting: true,
    }),
    react(),
    babel({
      // Explicit parser options after moving to @vitejs/plugin-react v6.0.0;
      // the v6 babel plugin only auto-parses TS/JSX for relative paths.
      parserOpts: { plugins: ["typescript", "jsx"] },
      presets: [reactCompilerPreset()],
    }),
    tailwindcss(),
    centralIconPrunePlugin(),
  ],
  resolve: {
    // Path-identical shims: `@t3tools/contracts` / `@t3tools/shared` resolve
    // to local bridge packages so the cloned MCode UI keeps its imports
    // verbatim. See docs/CONTRACTS-BRIDGE-DESIGN.md §3.1.
    // T1 aliases (`@/`) and MCode aliases (`~/`) are both preserved.
    // `@t3tools/shared` uses subpath imports (`@t3tools/shared/model`), so we
    // match the prefix and rewrite to `./src/shared/...`.
    alias: [
      { find: "@t3tools/shared", replacement: path.resolve(__dirname, "./src/shared") },
      { find: /^@t3tools\/shared\/(.+)$/, replacement: path.resolve(__dirname, "./src/shared/$1") },
      { find: "@t3tools/contracts", replacement: path.resolve(__dirname, "./src/contracts") },
      { find: "@", replacement: path.resolve(__dirname, "./src") },
      { find: "~", replacement: path.resolve(__dirname, "./src") },
    ],
  },
  optimizeDeps: {
    include: [
      "@pierre/diffs",
      "@pierre/diffs/react",
      "@pierre/diffs/worker/worker.js",
      "react-icons/gr",
    ],
  },
  define: {
    "import.meta.env.VITE_WS_URL": JSON.stringify(process.env.VITE_WS_URL ?? ""),
    "import.meta.env.APP_VERSION": JSON.stringify(process.env.npm_package_version ?? "0.1.0"),
  },
  clearScreen: false,
  server: {
    port,
    strictPort: true,
    host: host || "0.0.0.0",
    hmr: host
      ? { protocol: "ws", host, port: 5174 }
      : { protocol: "ws", host: "localhost" },
    watch: {
      ignored: ["**/src-tauri/**"],
    },
  },
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: buildSourcemap,
  },
});
