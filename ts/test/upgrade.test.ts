// The golden vector the Go tests check too, computed with Python's hmac rather
// than by either implementation.

import { describe, expect, test } from "bun:test";
import { parseUpgradeKey, signUpgrade, UpgradeVerifier } from "../src/upgrade.ts";

const goldenKey = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
const now = 1760000000;

describe("signed upgrade", () => {
  test("matches the golden vector", async () => {
    const auth = await signUpgrade(parseUpgradeKey(goldenKey), "Example.Workers.Dev", "/tunnel?pod=a", now, "AAAAAAAAAAAAAAAAAAAAAA");
    expect(auth).toBe("VTunnel-HMAC v1 ts=1760000000,nonce=AAAAAAAAAAAAAAAAAAAAAA,sig=6iNrjxS_e-78YQFJiYsLipkW842_ebLjaLgPIUR38EI");
  });

  test("keys", () => {
    for (const ok of [goldenKey, goldenKey.replace(/=+$/, ""), "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"]) {
      expect(parseUpgradeKey(ok).length).toBe(32);
    }
    for (const bad of ["", "short", "AAECAwQFBgcICQoLDA0ODw==", "not base64 at all!!"]) {
      expect(() => parseUpgradeKey(bad)).toThrow();
    }
  });

  test("verifier", async () => {
    const key = parseUpgradeKey(goldenKey);
    const v = new UpgradeVerifier(goldenKey, () => now);
    const sign = (host: string, path: string, at = now) => signUpgrade(key, host, path, at);

    const good = await sign("tunnel.example", "/tunnel");
    expect(await v.check(good, "TUNNEL.example", "/tunnel")).toBeNull();
    expect(await v.check(good, "tunnel.example", "/tunnel")).toBe("replay");

    const cases: [string, Promise<string> | string, string, string, string | null][] = [
      ["another host", sign("tunnel.example", "/tunnel"), "evil.example", "/tunnel", "bad_sig"],
      ["another path", sign("tunnel.example", "/tunnel"), "tunnel.example", "/tunnel?x", "bad_sig"],
      ["too old", sign("tunnel.example", "/tunnel", now - 61), "tunnel.example", "/tunnel", "expired"],
      ["from the future", sign("tunnel.example", "/tunnel", now + 61), "tunnel.example", "/tunnel", "expired"],
      ["just inside", sign("tunnel.example", "/tunnel", now - 59), "tunnel.example", "/tunnel", null],
      ["another scheme", "Bearer x", "tunnel.example", "/tunnel", "malformed"],
      ["short nonce", "VTunnel-HMAC v1 ts=1760000000,nonce=abc,sig=x", "tunnel.example", "/tunnel", "malformed"],
      ["another key", signUpgrade(new Uint8Array(32), "tunnel.example", "/tunnel", now), "tunnel.example", "/tunnel", "bad_sig"],
    ];
    for (const [name, auth, host, path, want] of cases) {
      expect([name, await v.check(await auth, host, path)]).toEqual([name, want]);
    }
    expect(await v.check(null, "tunnel.example", "/tunnel")).toBe("missing");
  });

  test("verify reads host and path from the request", async () => {
    const v = new UpgradeVerifier(goldenKey);
    const auth = await signUpgrade(parseUpgradeKey(goldenKey), "edge.example:8443", "/tunnel?pod=a");
    expect(await v.verify(new Request("https://edge.example:8443/tunnel?pod=a", { headers: { authorization: auth } }))).toBeNull();
  });
});
