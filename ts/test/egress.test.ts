// TunnelEgress against a TunnelServer with no controller process behind it:
// what fetch() does with a request it cannot carry through.

import { describe, expect, test } from "bun:test";
import { TunnelEgress } from "../src/egress.ts";
import { encodeFrame, FrameReader, type StreamReply } from "../src/frame.ts";
import { TunnelServer } from "../src/server.ts";
import type { Session } from "../src/yamux.ts";
import { pair, tick } from "./helpers.ts";

const quiet = { log: () => {} };

async function listen(session: Session, domains: string[]): Promise<StreamReply> {
  const s = await session.open();
  const w = s.writable.getWriter();
  await w.write(encodeFrame({ type: "listen", domains }));
  const reply = await new FrameReader(s.readable.getReader()).read<StreamReply>(1000);
  await w.close();
  return reply;
}

// An upload that never gets anywhere, and says whether it was let go of.
function stalled() {
  const state = { cancelled: false };
  const body = new ReadableStream<Uint8Array>({
    pull: () => new Promise(() => {}),
    cancel: () => {
      state.cancelled = true;
    },
  });
  const request = new Request("https://api.corp/", { method: "POST", body, duplex: "half" } as RequestInit);
  return { state, request };
}

describe("a fetch that fails lets go of the request body", () => {
  test("when the tunnel cannot be opened, for whatever reason", async () => {
    const server = { openTunnel: () => Promise.reject(new Error("session shutdown")) } as unknown as TunnelServer;
    const { state, request } = stalled();
    await expect(new TunnelEgress(server).fetch(request)).rejects.toThrow("session shutdown");
    await tick();
    expect(state.cancelled).toBe(true);
  });

  test("when its session is replaced with the body on its way", async () => {
    const server = new TunnelServer(quiet);
    const old = pair();
    const now = pair();
    void server.attach(old.server, "old", { client: "c1" });
    await listen(old.client, ["api.corp"]);

    const { state, request } = stalled();
    const res = new TunnelEgress(server).fetch(request);
    res.catch(() => {});
    await old.client.accept(); // the call reached the old session
    void server.attach(now.server, "new", { client: "c1" });

    await expect(res).rejects.toMatchObject({ code: "session_shutdown" });
    await tick();
    expect(state.cancelled).toBe(true);
  });

  test("when no answer comes in time", async () => {
    const server = new TunnelServer(quiet);
    const p = pair();
    void server.attach(p.server, "a");
    await listen(p.client, ["api.corp"]);

    const { state, request } = stalled();
    await expect(new TunnelEgress(server, { responseHeaderTimeoutMs: 20 }).fetch(request)).rejects.toThrow();
    await tick();
    expect(state.cancelled).toBe(true);
  });
});
