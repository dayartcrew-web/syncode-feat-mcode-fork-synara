// Vitest config — merges with vite.config.ts and adds the test environment.
//
// The `test` field is a vitest extension to Vite's UserConfig; keeping it in a
// separate file avoids TS2769 ("'test' does not exist in type
// 'UserConfigExport'") when `tsc --noEmit` type-checks vite.config.ts.
//
// `setupFiles` loads `src/test/setup.ts` which polyfills `navigator.userAgent`
// for vendored deps (e.g. @pierre/diffs CodeView) that read it at module-load
// time in the Node test environment.

import { defineConfig, mergeConfig } from "vitest/config";
import viteConfig from "./vite.config";

export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      setupFiles: ["./src/test/setup.ts"],
    },
  }),
);
