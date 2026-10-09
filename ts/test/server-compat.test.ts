// TunnelServer against a real vtunnel.Client (testdata/vtclient): the
// controller declares its routes, sets a policy and reconnects as it would
// against a sandbox, and its MITM proxy answers what comes down the tunnel.
// The bytes on the tunnel are written by hand here; egress-compat.test.ts does
// the same through Egress. Needs a Go toolchain.

import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import type { Stream } from "../src/yamux.ts";
import { buildController, Harness, removeController } from "./controller.ts";
import { str, text, tick } from "./helpers.ts";

beforeAll(buildController, 120_000);
afterAll(removeController);

let h: Harness;
afterEach(() => h.stop());

/**
 * Writes request and returns everything that comes back before the proxy
 * closes. The write side stays open until then: Go's net/http cancels a
 * request whose client half-closes, so an HTTP client must not FIN early.
 */
async function exchange(stream: Stream, request: string): Promise<string> {
  const w = stream.writable.getWriter();
  await w.write(text(request));
  let out = "";
  for await (const chunk of stream.readable) out += str(chunk);
  await w.close();
  return out;
}

function body(response: string): Record<string, unknown> {
  return JSON.parse(response.slice(response.indexOf("\r\n\r\n") + 4));
}

describe("TunnelServer against vtunnel.Client", () => {
  test("a proxied request in absolute form reaches the upstream, credential injected", async () => {
    h = await new Harness().start();
    const t = await h.server.openTunnel("fwd.test:80");
    const res = await exchange(t.stream, "GET http://fwd.test/hello?x=1 HTTP/1.1\r\nHost: fwd.test\r\nConnection: close\r\n\r\n");
    expect(res).toStartWith("HTTP/1.1 200");
    expect(body(res)).toMatchObject({
      who: "upstream",
      method: "GET",
      host: "fwd.test",
      uri: "/hello?x=1",
      authorization: "Bearer injected",
    });
  });

  test("CONNECT, then cleartext HTTP inside it, reaches a handler", async () => {
    h = await new Harness().start();
    const t = await h.server.openTunnel("api.test:443");
    const res = await exchange(
      t.stream,
      "CONNECT api.test:443 HTTP/1.1\r\nHost: api.test:443\r\n\r\n" +
        "POST /things HTTP/1.1\r\nHost: api.test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
    );
    expect(res).toStartWith("HTTP/1.1 200");
    const inner = res.slice(res.indexOf("\r\n\r\n") + 4);
    expect(inner).toStartWith("HTTP/1.1 200");
    expect(body(inner)).toMatchObject({ who: "handler", method: "POST", host: "api.test", uri: "/things" });
  });

  test("the policy got through: the controller is still alive", async () => {
    h = await new Harness().start();
    await tick(200);
    expect(h.procs[0]!.exitCode).toBeNull();
  });

  test("after a reconnect the controller replays its routes, on the same port", async () => {
    h = await new Harness().start();
    const before = await h.server.openTunnel("fwd.test:80");
    before.stream.reset();
    h.ws.sessions[0]!.close(); // the Durable Object dropped the socket
    await h.until(() => h.ws.sessions.length === 2 && h.server.sessions === 1 && h.server.routes("fwd.test:80"));
    const after = await h.server.openTunnel("fwd.test:80");
    expect(after.port).toBe(before.port);
    expect(after.session).not.toBe(before.session); // the new connection
    const res = await exchange(after.stream, "GET http://fwd.test/again HTTP/1.1\r\nHost: fwd.test\r\nConnection: close\r\n\r\n");
    expect(body(res)).toMatchObject({ uri: "/again", authorization: "Bearer injected" });
  }, 20_000);

  test("three controllers share the load; one dies, the others carry on", async () => {
    h = await new Harness().start(3);
    const seen = new Set<string>();
    for (let i = 0; i < 6; i++) {
      const t = await h.server.openTunnel("fwd.test:80");
      seen.add(t.session);
      const res = await exchange(t.stream, `GET http://fwd.test/${i} HTTP/1.1\r\nHost: fwd.test\r\nConnection: close\r\n\r\n`);
      expect(body(res).uri).toBe(`/${i}`);
    }
    expect(seen.size).toBe(3);

    h.procs[0]!.kill("SIGKILL"); // a pod gone without a goodbye
    await h.until(() => h.server.sessions === 2);
    for (let i = 0; i < 4; i++) {
      const t = await h.server.openTunnel("fwd.test:80");
      const res = await exchange(t.stream, "GET http://fwd.test/x HTTP/1.1\r\nHost: fwd.test\r\nConnection: close\r\n\r\n");
      expect(res).toStartWith("HTTP/1.1 200");
    }
  }, 30_000);
});
