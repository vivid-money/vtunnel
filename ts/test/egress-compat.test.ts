// Egress against a real controller (testdata/vtclient): fetch() and connect()
// as code in the isolate would call them, answered by vtunnel's MITM proxy and
// the upstreams behind it. Needs a Go toolchain.

import { afterAll, afterEach, beforeAll, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { ConnectError } from "../src/egress.ts";
import { buildController, Harness, removeController } from "./controller.ts";
import { randomBytes, readAll, str, text } from "./helpers.ts";

beforeAll(buildController, 120_000);
afterAll(removeController);

let h: Harness;
beforeAll(async () => {
  // One controller for the whole file: these tests do not disturb it.
  buildController();
  h = await new Harness().start();
}, 120_000);
afterAll(() => h.stop());

const sha256 = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");

describe("fetch", () => {
  test("GET with a query, header passed, credential injected, fragment dropped", async () => {
    const res = await h.egress.fetch(new Request("http://fwd.test/a/b?x=1&y=%20#frag", { headers: { "X-Test": "yes" } }));
    expect(res.status).toBe(200);
    expect(res.headers.get("content-type")).toBe("application/json");
    expect(await res.json()).toMatchObject({
      who: "upstream",
      method: "GET",
      host: "fwd.test",
      uri: "/a/b?x=1&y=%20",
      "x-test": "yes",
      authorization: "Bearer injected",
      bodyLength: 0,
    });
  });

  test("https: crosses the tunnel in the clear, the controller does the TLS", async () => {
    const res = await h.egress.fetch(new Request("https://tls.test/secure"));
    expect(await res.json()).toMatchObject({ uri: "/secure", tls: true, authorization: "Bearer injected-tls" });
  });

  test("POST with a body of known length", async () => {
    // workerd hands the gateway a Request with Content-Length set; Bun,
    // following the Fetch spec, does not, so it is set here.
    const body = randomBytes(300_000);
    const res = await h.egress.fetch(
      new Request("http://fwd.test/up", { method: "POST", body, headers: { "content-length": "300000" } }),
    );
    expect(await res.json()).toMatchObject({
      method: "POST",
      transferEncoding: null,
      contentLength: 300_000,
      bodyLength: 300_000,
      bodySha256: sha256(body),
    });
  });

  test("a body longer than its Content-Length fails the fetch instead of being cut", async () => {
    const res = h.egress.fetch(
      new Request("http://fwd.test/liar", { method: "POST", body: "12345", headers: { "content-length": "3" } }),
    );
    await expect(res).rejects.toThrow();
  });

  test("POST with a streamed body of unknown length goes chunked", async () => {
    const parts = Array.from({ length: 40 }, () => randomBytes(128 * 1024));
    const stream = new ReadableStream<Uint8Array>({
      pull(c) {
        const next = parts.shift();
        if (next) c.enqueue(next);
        else c.close();
      },
    });
    const all = parts.flatMap((p) => [...p]);
    const res = await h.egress.fetch(new Request("http://fwd.test/stream", { method: "PUT", body: stream, duplex: "half" } as RequestInit));
    const got = (await res.json()) as Record<string, unknown>;
    expect(got).toMatchObject({ method: "PUT", transferEncoding: ["chunked"], bodyLength: all.length });
    expect(got.bodySha256).toBe(sha256(Uint8Array.from(all)));
  });

  test("POST without a body says Content-Length: 0", async () => {
    const res = await h.egress.fetch(new Request("http://fwd.test/empty", { method: "POST" }));
    expect(await res.json()).toMatchObject({ method: "POST", contentLength: 0 });
  });

  test("a large response streams", async () => {
    const n = 20 * 1024 * 1024 + 3;
    const res = await h.egress.fetch(new Request(`http://api.test/big?n=${n}`));
    expect(res.headers.get("content-length")).toBe(String(n));
    let total = 0;
    let ok = true;
    for await (const chunk of res.body!) {
      for (let i = 0; i < chunk.length; i++) if (chunk[i] !== ((total + i) % (64 * 1024)) % 256) ok = false;
      total += chunk.length;
    }
    expect(total).toBe(n);
    expect(ok).toBe(true);
  }, 30_000);

  test("chunked response with trailers", async () => {
    const res = await h.egress.fetch(new Request("http://api.test/chunked"));
    expect(await res.text()).toBe("part 0;part 1;part 2;part 3;part 4;");
  });

  test("SSE: events arrive one by one, not at the end", async () => {
    const res = await h.egress.fetch(new Request("http://api.test/sse"));
    expect(res.headers.get("content-type")).toBe("text/event-stream");
    const start = Date.now();
    const arrivals: [number, string][] = [];
    for await (const chunk of res.body!) arrivals.push([Date.now() - start, str(chunk)]);
    expect(arrivals.map(([, s]) => s).join("")).toBe("data: 0\n\ndata: 1\n\ndata: 2\n\n");
    expect(arrivals.length).toBeGreaterThanOrEqual(3);
    expect(arrivals[0]![0]).toBeLessThan(150); // the first event did not wait for the last
  });

  test("statuses without a body, and with one", async () => {
    for (const [status, hasBody] of [[204, false], [304, false], [404, true], [500, true], [201, true]] as const) {
      const res = await h.egress.fetch(new Request(`http://api.test/status/${status}`));
      expect([status, res.status]).toEqual([status, status]);
      expect(res.headers.get("x-status")).toBe(String(status));
      expect(await res.text()).toBe(hasBody ? "status body" : "");
    }
  });

  test("HEAD has no body, whatever its Content-Length says", async () => {
    const res = await h.egress.fetch(new Request("http://api.test/big?n=1000", { method: "HEAD" }));
    expect(res.status).toBe(200);
    expect(res.headers.get("content-length")).toBe("1000");
    expect(res.body).toBeNull();
  });

  test("repeated headers survive", async () => {
    const res = await h.egress.fetch(new Request("http://api.test/cookies"));
    expect(res.headers.getSetCookie()).toEqual(["a=1", "b=2"]);
  });

  test("hop-by-hop headers from the application are not passed on", async () => {
    const res = await h.egress.fetch(
      new Request("http://api.test/", { headers: { Connection: "keep-alive, X-Test", "X-Test": "should-stay", TE: "trailers" } }),
    );
    expect(await res.json()).toMatchObject({ who: "handler", "x-test": "should-stay" });
  });

  test("no route: a 502 made here, nothing sent", async () => {
    const res = await h.egress.fetch(new Request("https://elsewhere.test/"));
    expect(res.status).toBe(502);
    expect(res.headers.get("x-vtunnel-error")).toBe("no-route");
    expect(await res.text()).toContain("no route for elsewhere.test:443");
  });

  test("abort mid-response fails the body", async () => {
    const ac = new AbortController();
    const res = await h.egress.fetch(new Request("http://api.test/sse", { signal: ac.signal }));
    const reader = res.body!.getReader();
    expect(str((await reader.read()).value!)).toBe("data: 0\n\n");
    ac.abort();
    await expect(reader.read()).rejects.toMatchObject({ code: "connection_reset" });
  });

  test("50 at once", async () => {
    const results = await Promise.all(
      Array.from({ length: 50 }, async (_, i) => {
        const res = await h.egress.fetch(new Request(`http://fwd.test/n/${i}`, { method: "POST", body: `body ${i}` }));
        return ((await res.json()) as { uri: string }).uri === `/n/${i}`;
      }),
    );
    expect(results.every(Boolean)).toBe(true);
  });
});

describe("connect", () => {
  // A socket as connect() would hand one to the application: what the
  // application writes goes to the tunnel, what the tunnel sends it reads.
  function socketPair() {
    const toTunnel = new TransformStream<Uint8Array, Uint8Array>();
    const fromTunnel = new TransformStream<Uint8Array, Uint8Array>();
    return {
      tunnelSide: { readable: toTunnel.readable, writable: fromTunnel.writable },
      app: { writer: toTunnel.writable.getWriter(), readable: fromTunnel.readable },
    };
  }

  test("raw TCP: server speaks first, bytes both ways, half-close answered", async () => {
    const { tunnelSide, app } = socketPair();
    const piping = h.egress.connect("tcp.test:7000", tunnelSide);
    await app.writer.write(text("ping\n"));
    await app.writer.close(); // FIN: the echo server answers it with BYE
    expect(str(await readAll(app.readable))).toBe("HELLO\nping\nBYE\n");
    await piping;
  });

  test("no route: refused before anything is sent", async () => {
    const { tunnelSide } = socketPair();
    await expect(h.egress.connect("db.elsewhere:5432", tunnelSide)).rejects.toBeInstanceOf(ConnectError);
  });
});
