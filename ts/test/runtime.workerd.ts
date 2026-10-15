// Run only by vitest.config.ts (no .test. in the name, so bun test skips it):
// proof that suite is executing inside workerd, not in Node pretending.
import { expect, test } from "vitest";

test("running in workerd", () => {
  expect(navigator.userAgent).toBe("Cloudflare-Workers");
});
