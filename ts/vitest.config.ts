// The runtime-agnostic tests, run inside workerd — the runtime the library
// ships to — rather than under Bun. The tests that start Go processes need a
// machine and stay on Bun; the workerd stand in e2e/workerd runs that path
// for real.
//
//	npx vitest run
import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [
    cloudflareTest({
      miniflare: {
        compatibilityDate: "2026-10-01",
        compatibilityFlags: ["nodejs_compat"],
      },
    }),
  ],
  test: {
    include: [
      "test/bytes.test.ts",
      "test/frame.test.ts",
      "test/http1.test.ts",
      "test/routes.test.ts",
      "test/runtime.workerd.ts",
      "test/server.test.ts",
      "test/upgrade.test.ts",
      "test/yamux.test.ts",
    ],
  },
});
