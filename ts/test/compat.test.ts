// yamux against the Go end vtunnel actually runs: testdata/yamuxpeer dials a
// WebSocket served here and speaks yamux-insecure through internal/transport/ws
// and internal/session. Needs a Go toolchain.

import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { Session, SessionOptions } from "../src/yamux.ts";
import { randomBytes, readAll, str, text, writeAll } from "./helpers.ts";
import { buildGoHelper, serveSessions } from "./wsserver.ts";

let peer: { bin: string; cleanup: () => void };
beforeAll(() => {
  peer = buildGoHelper("yamuxpeer");
}, 120_000);
afterAll(() => peer.cleanup());

/**
 * Runs the Go peer's scenario against a yamux server session handed to
 * onSession. Resolves with the peer's exit code and stderr once both the peer
 * and onSession are done.
 */
async function run(
  scenario: string,
  onSession: (s: Session) => Promise<void>,
  opts: SessionOptions = {},
): Promise<{ code: number; stderr: string }> {
  let sessionDone!: Promise<void>;
  const ready = Promise.withResolvers<void>();
  const server = serveSessions((s) => {
    sessionDone = onSession(s);
    ready.resolve();
  }, opts);
  try {
    const proc = Bun.spawn([peer.bin, "-url", server.url, "-case", scenario], { stderr: "pipe" });
    const [code, stderr] = await Promise.all([proc.exited, new Response(proc.stderr).text()]);
    await ready.promise;
    await sessionDone;
    return { code, stderr };
  } finally {
    server.stop();
  }
}

describe("yamux against Go (hashicorp/yamux v0.1.2)", () => {
  test("Go opens 300 streams, TS echoes; windows smaller than payloads", async () => {
    let echoed = 0;
    let rtt = -1;
    const res = await run("echo", async (session) => {
      rtt = await session.ping();
      const pipes: Promise<void>[] = [];
      for await (const s of session) {
        pipes.push(s.readable.pipeTo(s.writable).then(() => void echoed++));
      }
      await Promise.all(pipes);
    });
    expect(res.stderr).toBe("");
    expect(res.code).toBe(0);
    expect(echoed).toBe(300);
    expect(rtt).toBeGreaterThanOrEqual(0);
  }, 60_000);

  test("same with the Go-sized 8 MiB window", async () => {
    const res = await run(
      "echo",
      async (session) => {
        for await (const s of session) void s.readable.pipeTo(s.writable);
      },
      { maxStreamWindow: 8 * 1024 * 1024 },
    );
    expect(res.stderr).toBe("");
    expect(res.code).toBe(0);
  }, 60_000);

  test("TS opens 50 streams, Go echoes; opens past the backlog need Go's ACKs", async () => {
    const results: boolean[] = [];
    const res = await run("serveropen", async (session) => {
      await Promise.all(
        Array.from({ length: 50 }, async (_, i) => {
          const s = await session.open();
          const data = randomBytes(i * 50_000);
          const [, got] = await Promise.all([writeAll(s, data), readAll(s.readable)]);
          results.push(Buffer.compare(got, data) === 0);
        }),
      );
      session.close();
    }, { acceptBacklog: 8 });
    expect(res.stderr).toBe("");
    expect(res.code).toBe(0);
    expect(results).toHaveLength(50);
    expect(results.every(Boolean)).toBe(true);
  }, 60_000);

  test("slow reader: Go fills the TS window to the byte and waits", async () => {
    let total = 0;
    const res = await run(
      "bulk",
      async (session) => {
        const s = await session.accept();
        const r = s.readable.getReader();
        const w = s.writable.getWriter();
        await Bun.sleep(200); // Go has used up the whole window by now
        for (let i = 0; ; i++) {
          const { value, done } = await r.read();
          if (done) break;
          total += value.length;
          await w.write(value);
          if (i % 64 === 0) await Bun.sleep(5);
        }
        await w.close();
        await session.closed;
      },
      { maxStreamWindow: 256 * 1024 },
    );
    expect(res.stderr).toBe("");
    expect(res.code).toBe(0);
    expect(total).toBe(16 << 20);
  }, 60_000);

  test("Go holds 300 streams open at once: needs TS's ACKs", async () => {
    const res = await run("hold", async (session) => {
      for await (const s of session) void s.readable.pipeTo(s.writable);
    });
    expect(res.stderr).toBe("");
    expect(res.code).toBe(0);
  }, 30_000);

  test("TS holds 20 streams open at once past a backlog of 8: needs Go's ACKs", async () => {
    const res = await run(
      "serveropen",
      async (session) => {
        const streams = [];
        for (let i = 0; i < 20; i++) {
          const s = await session.open();
          const w = s.writable.getWriter();
          await w.write(Uint8Array.of(i));
          streams.push({ s, w, r: s.readable.getReader() });
        }
        for (const [i, { r }] of streams.entries()) expect((await r.read()).value![0]).toBe(i);
        session.close();
      },
      { acceptBacklog: 8 },
    );
    expect(res.stderr).toBe("");
    expect(res.code).toBe(0);
  }, 30_000);

  test("TS reset reaches Go as connection reset", async () => {
    const res = await run("reset", async (session) => {
      const s = await session.accept();
      const r = s.readable.getReader();
      expect(str((await r.read()).value!)).toBe("reset me");
      s.reset();
      await session.closed;
    });
    expect(res.stderr).toBe("");
    expect(res.code).toBe(0);
  }, 30_000);

  test("Go process dies mid-stream: pending read fails, session closes", async () => {
    let readErr: unknown;
    const res = await run("die", async (session) => {
      const s = await session.accept();
      const r = s.readable.getReader();
      expect(str((await r.read()).value!)).toBe("half a request");
      await s.writable.getWriter().write(text("ready"));
      readErr = await r.read().catch((e) => e);
      await session.closed;
    });
    expect(res.code).toBe(0);
    expect(readErr).toMatchObject({ code: "session_shutdown" });
  }, 30_000);
});
