// The application under test: a dynamic Worker whose only network is the
// gateway. Each case does what an application would and answers with what it
// saw, as JSON; the Go test decides whether that is right.

import { connect } from "cloudflare:sockets";

const enc = new TextEncoder();
const dec = new TextDecoder();

async function readAll(stream) {
  let out = "";
  for await (const chunk of stream) out += dec.decode(chunk, { stream: true });
  return out;
}

async function sha256(bytes) {
  const d = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

const cases = {
  // fetch: plain, with the credential injected by the controller.
  async get() {
    const r = await fetch("http://fwd.test/a/b?x=1#frag", { headers: { "X-Test": "yes" } });
    return { status: r.status, body: await r.json() };
  },

  // fetch https: the request reaches the gateway before any TLS.
  async https() {
    const r = await fetch("https://tls.test/secure");
    return { status: r.status, body: await r.json() };
  },

  async post(q) {
    const n = Number(q.get("n") ?? 300000);
    const body = new Uint8Array(n);
    for (let i = 0; i < n; i++) body[i] = i % 251;
    const r = await fetch("http://fwd.test/up", { method: "POST", body });
    return { status: r.status, body: await r.json(), sha256: await sha256(body) };
  },

  async stream(q) {
    const parts = Number(q.get("parts") ?? 40);
    let i = 0;
    const body = new ReadableStream({
      pull(c) {
        if (i++ < parts) c.enqueue(new Uint8Array(128 * 1024).fill(i));
        else c.close();
      },
    });
    const r = await fetch("http://fwd.test/stream", { method: "PUT", body, duplex: "half" });
    return { status: r.status, body: await r.json() };
  },

  async big(q) {
    const n = Number(q.get("n") ?? 20 * 1024 * 1024);
    const r = await fetch(`http://api.test/big?n=${n}`);
    let total = 0;
    for await (const chunk of r.body) total += chunk.length;
    return { status: r.status, total, contentLength: r.headers.get("content-length") };
  },

  // A download that may be cut short: reports how far it got and how it ended.
  async cut(q) {
    const n = Number(q.get("n"));
    let total = 0;
    try {
      const r = await fetch(`http://api.test/big?n=${n}`);
      for await (const chunk of r.body) total += chunk.length;
      return { total, error: null };
    } catch (e) {
      return { total, error: String(e) };
    }
  },

  async sse() {
    const r = await fetch("http://api.test/sse");
    const start = Date.now();
    const arrivals = [];
    for await (const chunk of r.body) arrivals.push([Date.now() - start, dec.decode(chunk)]);
    return { status: r.status, type: r.headers.get("content-type"), arrivals };
  },

  async statuses() {
    const out = {};
    for (const s of [204, 304, 404, 500]) {
      const r = await fetch(`http://api.test/status/${s}`);
      out[s] = { status: r.status, body: await r.text() };
    }
    return out;
  },

  async head() {
    const r = await fetch("http://api.test/big?n=1000", { method: "HEAD" });
    return { status: r.status, contentLength: r.headers.get("content-length"), body: await r.text() };
  },

  async trailers() {
    const r = await fetch("http://api.test/chunked");
    return { status: r.status, body: await r.text() };
  },

  async hopbyhop() {
    const r = await fetch("http://api.test/", {
      headers: { Connection: "keep-alive, X-Test", "X-Test": "should-stay", TE: "trailers", "Proxy-Authorization": "Basic eA==" },
    });
    return { status: r.status, body: await r.json() };
  },

  // Aborting mid-response fails the read that is waiting, and does not hang.
  async abort() {
    const ac = new AbortController();
    const r = await fetch("http://api.test/sse", { signal: ac.signal });
    const reader = r.body.getReader();
    const first = dec.decode((await reader.read()).value);
    ac.abort();
    try {
      await reader.read();
      return { first, error: null };
    } catch (e) {
      return { first, error: String(e) };
    }
  },

  // Giving up on a big body part way: the stream behind it must be closed.
  async cancel() {
    const r = await fetch(`http://api.test/big?n=${200 << 20}`);
    const reader = r.body.getReader();
    let got = 0;
    while (got < 1 << 20) got += (await reader.read()).value.length;
    await reader.cancel();
    return { got };
  },

  // A reader slower than the wire: flow control holds the sender back the
  // whole way, and every byte still arrives.
  async slow(q) {
    const n = Number(q.get("n") ?? 64 << 20);
    const r = await fetch(`http://api.test/big?n=${n}`);
    let total = 0;
    let i = 0;
    for await (const chunk of r.body) {
      total += chunk.length;
      if (++i % 64 === 0) await scheduler.wait(5);
    }
    return { total };
  },

  async slowget() {
    const r = await fetch("http://api.test/slow");
    return { status: r.status, body: await r.text() };
  },

  async cookies() {
    const r = await fetch("http://api.test/cookies");
    return { cookies: r.headers.getSetCookie() };
  },

  async noroute() {
    const r = await fetch("https://example.com/");
    return { status: r.status, error: r.headers.get("x-vtunnel-error"), body: await r.text() };
  },

  async parallel(q) {
    const n = Number(q.get("n") ?? 50);
    const got = await Promise.all(
      Array.from({ length: n }, async (_, i) => {
        const r = await fetch(`http://fwd.test/n/${i}`, { method: "POST", body: `body ${i}` });
        return (await r.json()).uri === `/n/${i}`;
      }),
    );
    return { ok: got.filter(Boolean).length, n };
  },

  // connect: raw TCP to the echo server, half-close answered.
  async tcp() {
    const s = connect("tcp.test:7000", { allowHalfOpen: true });
    const w = s.writable.getWriter();
    await w.write(enc.encode("ping\n"));
    await w.close();
    return { got: await readAll(s.readable) };
  },

  async tcpnoroute() {
    try {
      const s = connect("db.elsewhere:5432");
      await s.opened;
      const got = await readAll(s.readable);
      return { error: null, got };
    } catch (e) {
      return { error: String(e) };
    }
  },

  async tcptls() {
    try {
      const s = connect("tcp.test:443", { secureTransport: "on" });
      await s.opened;
      return { error: null };
    } catch (e) {
      return { error: String(e) };
    }
  },

  // Throughput, for the benchmark: bytes through fetch or connect, timed here.
  async download(q) {
    const n = Number(q.get("n"));
    const start = Date.now();
    const r = await fetch(`http://api.test/big?n=${n}`);
    let total = 0;
    for await (const chunk of r.body) total += chunk.length;
    return { total, ms: Date.now() - start };
  },

  async upload(q) {
    const n = Number(q.get("n"));
    const chunk = new Uint8Array(64 * 1024);
    let sent = 0;
    const body = new ReadableStream({
      pull(c) {
        if (sent >= n) return c.close();
        const k = Math.min(chunk.length, n - sent);
        sent += k;
        c.enqueue(chunk.subarray(0, k));
      },
    });
    const start = Date.now();
    const r = await fetch("http://sink.test/", { method: "POST", body, duplex: "half" });
    const got = await r.json();
    return { total: got.n, ms: Date.now() - start };
  },

  async tcpdownload(q) {
    const n = Number(q.get("n"));
    const start = Date.now();
    const s = connect("source.test:9");
    let total = 0;
    const r = s.readable.getReader();
    while (total < n) {
      const { value, done } = await r.read();
      if (done) break;
      total += value.length;
    }
    await s.close();
    return { total, ms: Date.now() - start };
  },

  async requests(q) {
    const n = Number(q.get("n"));
    const c = Number(q.get("c") ?? 1);
    const start = Date.now();
    let i = 0;
    await Promise.all(
      Array.from({ length: c }, async () => {
        while (i++ < n) await (await fetch("http://api.test/status/204")).arrayBuffer();
      }),
    );
    return { n, ms: Date.now() - start };
  },
};

export default {
  async fetch(request) {
    const q = new URL(request.url).searchParams;
    const run = cases[q.get("case")];
    if (!run) return new Response("unknown case", { status: 400 });
    try {
      return Response.json(await run(q));
    } catch (e) {
      return Response.json({ threw: String(e?.stack ?? e) }, { status: 500 });
    }
  },
};
