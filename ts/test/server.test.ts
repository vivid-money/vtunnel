import { describe, expect, test } from "vitest";
import { encodeFrame, FrameReader, type StreamHeader, type StreamReply } from "../src/frame.ts";
import { NoRouteError, TunnelServer } from "../src/server.ts";
import type { Session } from "../src/yamux.ts";
import { pair, readAll, str, text, tick, writeAll } from "./helpers.ts";

// What the Go client's request() does: one header out, one reply back.
async function request(session: Session, h: unknown): Promise<StreamReply> {
  const s = await session.open();
  const w = s.writable.getWriter();
  await w.write(encodeFrame(h));
  const reply = await new FrameReader(s.readable.getReader()).read<StreamReply>(1000);
  await w.close();
  return reply;
}

// A controller: a client session attached to server, answering pings.
function controller(server: TunnelServer, label: string) {
  const p = pair();
  const served = server.attach(p.server, label);
  return { session: p.client, served };
}

// Accepts the next tunnel stream on a controller session and reads its header.
async function nextTunnel(session: Session) {
  const s = await session.accept();
  const r = new FrameReader(s.readable.getReader());
  const header = await r.read<StreamHeader>(1000);
  return { s, header, body: r.rest() };
}

const quiet = { log: () => {} };

describe("control streams", () => {
  test("ping", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    expect(await request(session, { type: "ping" })).toEqual({ ok: true });
  });

  test("listen with port 0 allocates; with a port, takes it", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    expect(await request(session, { type: "listen", domains: ["api.corp"] })).toEqual({ ok: true, port: 49152 });
    expect(await request(session, { type: "listen", port: 50000, domains: ["db.corp"] })).toEqual({
      ok: true,
      port: 50000,
    });
    expect(server.routes("api.corp:443")).toBe(true);
    expect(server.routes("db.corp:5432")).toBe(true);
  });

  test("listen replaces a port's domains, down to none", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    const { port } = await request(session, { type: "listen", domains: ["a.corp", "b.corp"] });
    await request(session, { type: "listen", port, domains: ["b.corp"] });
    expect(server.routes("a.corp:443")).toBe(false);
    expect(server.routes("b.corp:443")).toBe(true);
    await request(session, { type: "listen", port });
    expect(server.routes("b.corp:443")).toBe(false);
  });

  test("bad listen", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    expect((await request(session, { type: "listen", port: 70000 })).ok).toBe(false);
    expect((await request(session, { type: "listen", domains: "api.corp" })).ok).toBe(false);
  });

  test("policy: only deny-everything-direct is applied", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    expect(await request(session, { type: "policy", policy: { default: "deny" } })).toEqual({
      ok: true,
      policyApplied: true,
    });
    for (const policy of [
      { default: "allow" },
      { default: "deny", allow: ["github.com"] },
      { default: "sometimes" },
      undefined,
    ]) {
      const reply = await request(session, { type: "policy", policy });
      expect(reply.ok).toBe(false);
      expect(reply.policyApplied).toBeUndefined();
      expect(typeof reply.error).toBe("string");
    }
  });

  test("unknown type is answered, not dropped", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    expect(await request(session, { type: "teleport" })).toEqual({ ok: false, error: "unknown stream type" });
  });

  test("a silent stream is reset after the header timeout", async () => {
    const server = new TunnelServer({ ...quiet, headerTimeoutMs: 20 });
    const { session } = controller(server, "a");
    const s = await session.open();
    await s.writable.getWriter().write(new Uint8Array(0)); // SYN, no header
    await expect(s.readable.getReader().read()).rejects.toMatchObject({ code: "connection_reset" });
  });

  test("garbage header resets the stream", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    const s = await session.open();
    await s.writable.getWriter().write(encodeFrame(["not", "a", "header"]));
    await expect(s.readable.getReader().read()).rejects.toMatchObject({ code: "connection_reset" });
  });
});

describe("tunnels", () => {
  test("opens a tunnel stream with the port of the matching domain", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    const { port } = await request(session, { type: "listen", domains: ["*.corp"] });

    const opening = server.openTunnel("API.corp:443");
    const { s, header, body } = await nextTunnel(session);
    const t = await opening;
    expect(header).toEqual({ type: "tunnel", port: port! });
    expect(t).toMatchObject({ port, pattern: "*.corp", session: "a" });

    // The stream is the caller's from here: bytes both ways, half-close.
    await writeAll(t.stream, text("CONNECT api.corp:443 HTTP/1.1\r\n\r\n"));
    expect(str(await readAll(body))).toBe("CONNECT api.corp:443 HTTP/1.1\r\n\r\n");
    await writeAll(s, text("HTTP/1.1 200 OK\r\n\r\n"));
    expect(str(await readAll(t.stream.readable))).toBe("HTTP/1.1 200 OK\r\n\r\n");
  });

  test("no route: refused here, nothing opened", async () => {
    const server = new TunnelServer(quiet);
    const { session } = controller(server, "a");
    await request(session, { type: "listen", domains: ["api.corp:443"] });
    for (const hostPort of ["api.corp:80", "other.corp:443", "api.corp", ":443", "[::1]443"]) {
      await expect(server.openTunnel(hostPort)).rejects.toBeInstanceOf(NoRouteError);
    }
    expect(session.numStreams).toBe(0);
  });

  test("takes turns among sessions that route the host, and only those", async () => {
    const server = new TunnelServer(quiet);
    const a = controller(server, "a");
    const b = controller(server, "b");
    const c = controller(server, "c");
    await request(a.session, { type: "listen", domains: ["api.corp"] });
    await request(b.session, { type: "listen", domains: ["api.corp"] });
    await request(c.session, { type: "listen", domains: ["other.corp"] });
    const used = [];
    for (let i = 0; i < 4; i++) used.push((await server.openTunnel("api.corp:443")).session);
    expect(used.sort()).toEqual(["a", "a", "b", "b"]);
  });

  test("a session that goes away takes its routes with it", async () => {
    const server = new TunnelServer(quiet);
    const a = controller(server, "a");
    const b = controller(server, "b");
    await request(a.session, { type: "listen", domains: ["api.corp"] });
    await request(b.session, { type: "listen", domains: ["api.corp"] });
    a.session.close();
    await a.served;
    expect(server.sessions).toBe(1);
    for (let i = 0; i < 3; i++) expect((await server.openTunnel("api.corp:443")).session).toBe("b");
    b.session.close();
    await b.served;
    await expect(server.openTunnel("api.corp:443")).rejects.toBeInstanceOf(NoRouteError);
  });

  test("a tunnel in flight fails when its session dies", async () => {
    const server = new TunnelServer(quiet);
    const a = controller(server, "a");
    await request(a.session, { type: "listen", domains: ["api.corp"] });
    const t = await server.openTunnel("api.corp:443");
    await nextTunnel(a.session);
    const read = t.stream.readable.getReader().read().catch((e) => e);
    a.session.close();
    expect(await read).toMatchObject({ code: "session_shutdown" });
  });
});

describe("resuming and liveness", () => {
  test("routes given to attach are served at once; changes are reported whole", async () => {
    const server = new TunnelServer(quiet);
    const p = pair();
    const reported: [number, string[]][][] = [];
    void server.attach(p.server, "a", { routes: [[50000, ["api.corp"]]], onRoutes: (r) => reported.push(r) });
    expect(server.routes("api.corp:443")).toBe(true);

    const t = server.openTunnel("api.corp:443");
    const { header } = await nextTunnel(p.client);
    expect(header).toEqual({ type: "tunnel", port: 50000 });
    await t;

    await request(p.client, { type: "listen", domains: ["db.corp"] });
    expect(reported).toEqual([[[50000, ["api.corp"]], [49152, ["db.corp"]]]]);
  });

  test("a session its alive() gives up on is closed, and calls go elsewhere", async () => {
    const server = new TunnelServer(quiet);
    const dead = pair();
    const live = pair();
    let answering = true;
    void server.attach(dead.server, "dead", { routes: [[1, ["api.corp"]]], alive: () => answering });
    void server.attach(live.server, "live", { routes: [[2, ["api.corp"]]] });
    answering = false;
    for (let i = 0; i < 3; i++) expect((await server.openTunnel("api.corp:443")).session).toBe("live");
    expect(dead.server.isClosed).toBe(true);
  });
});

describe("keepalive", () => {
  test("pings the controller", async () => {
    const server = new TunnelServer({ ...quiet, keepAliveMs: 10 });
    const { session } = controller(server, "a");
    const s = await session.accept();
    expect(await new FrameReader(s.readable.getReader()).read<StreamHeader>()).toEqual({ type: "ping" });
  });

  test("an unanswered ping closes the session", async () => {
    const server = new TunnelServer({ ...quiet, keepAliveMs: 10 });
    const { session, served } = controller(server, "a");
    // The controller accepts the ping stream and never answers it.
    const held = session.accept();
    await served;
    expect(server.sessions).toBe(0);
    await held;
    await tick();
    expect(session.isClosed).toBe(true);
  });

  test("close ends every session and refuses new ones", async () => {
    const server = new TunnelServer(quiet);
    const a = controller(server, "a");
    server.close();
    await a.served;
    const late = pair();
    await server.attach(late.server, "late");
    expect(late.server.isClosed).toBe(true);
  });
});
