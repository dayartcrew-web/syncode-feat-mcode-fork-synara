// Vitest global setup — runs before every test file.
//
// Polyfills `navigator.userAgent` when missing (Node.js environment without
// jsdom/happy-dom). Some vendored dependencies (e.g. @pierre/diffs CodeView)
// read `navigator.userAgent` at module-load time, which crashes in plain Node
// where the global `navigator` object doesn't exist. We provide a minimal
// stub so the import succeeds and SSR tests (renderToStaticMarkup) can run
// without a full DOM environment.

if (typeof globalThis.navigator === "undefined") {
  // @ts-expect-error — navigator is read-only in the type defs, but Node.js
  // doesn't define it at all so we're free to assign.
  globalThis.navigator = {
    userAgent: "vitest-node",
    platform: process.platform,
  };
}
