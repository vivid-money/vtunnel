import { describe, expect, test } from "bun:test";
import { Session, YamuxError } from "../src/yamux.ts";
import { pair, rawFrame, randomBytes, readAll, str, text, tick, writeAll } from "./helpers.ts";

describe("streams", () => {
  test("open, accept, half-close both ways", async () => {
    const { client, server } = pair();
    const opened = await client.open();
    expect(opened.id).toBe(1);

    const serverSide = (async () => {
      const s = await server.accept();
      expect(s.id).toBe(1);
      const got = str(await readAll(s.readable));
      await writeAll(s, text(got.toUpperCase()));
    })();

    await writeAll(opened, text("hello"));
    expect(str(await readAll(opened.readable))).toBe("HELLO");
    await serverSide;
    await tick();
    expect(client.numStreams).toBe(0);
    expect(server.numStreams).toBe(0);
  });

  test("server opens even IDs", async () => {
    const { client, server } = pair();
    const s = await server.open();
    expect(s.id).toBe(2);
    expect((await server.open()).id).toBe(4);
    const c = await client.accept();
    expect(c.id).toBe(2);
  });

  test("reply survives our FIN: closing the writable does not end reading", async () => {
    const { client, server } = pair();
    const c = await client.open();
    await writeAll(c, text("ask"));
    const s = await server.accept();
    expect(str(await readAll(s.readable))).toBe("ask");
    await tick(10);
    await writeAll(s, text("answer"));
    expect(str(await readAll(c.readable))).toBe("answer");
  });

  test("data larger than any window, both directions at once", async () => {
    const { client, server } = pair();
    const up = randomBytes(5 * 1024 * 1024 + 7);
    const down = randomBytes(3 * 1024 * 1024 + 3);
    const c = await client.open();
    const serverSide = (async () => {
      const s = await server.accept();
      const [got] = await Promise.all([readAll(s.readable), writeAll(s, down)]);
      return got;
    })();
    const [, gotDown] = await Promise.all([writeAll(c, up), readAll(c.readable)]);
    expect(Buffer.compare(await serverSide, up)).toBe(0);
    expect(Buffer.compare(gotDown, down)).toBe(0);
  });

  test("a writer blocks once the reader's window is full, and resumes when it reads", async () => {
    const window = 256 * 1024;
    const { client, server } = pair({ server: { maxStreamWindow: window } });
    const c = await client.open();
    const s = await server.accept();
    await tick();

    const w = c.writable.getWriter();
    let wrote = false;
    const writing = w.write(randomBytes(window + 1000)).then(() => (wrote = true));
    await tick(20);
    expect(wrote).toBe(false); // 1000 bytes still have no window

    const r = s.readable.getReader();
    let read = 0;
    while (read < window + 1000) read += (await r.read()).value!.length;
    await writing;
    expect(wrote).toBe(true);
  });

  test("many concurrent streams, more than the backlog", async () => {
    const { client, server } = pair({ client: { acceptBacklog: 16 }, server: { acceptBacklog: 16 } });
    const echo = (async () => {
      for await (const s of server) {
        void s.readable.pipeTo(s.writable);
      }
    })();
    const results = await Promise.all(
      Array.from({ length: 300 }, async (_, i) => {
        const c = await client.open();
        const msg = `stream ${i} `.repeat(i % 50);
        const [, got] = await Promise.all([writeAll(c, text(msg)), readAll(c.readable)]);
        return str(got) === msg;
      }),
    );
    expect(results.every(Boolean)).toBe(true);
    client.close();
    await echo;
  });

  test("reset fails the peer's reads and writes", async () => {
    const { client, server } = pair();
    const c = await client.open();
    await c.writable.getWriter().write(text("x"));
    const s = await server.accept();
    c.reset();
    await tick();
    const r = s.readable.getReader();
    await expect(r.read()).rejects.toMatchObject({ code: "connection_reset" });
    await expect(s.writable.getWriter().write(text("y"))).rejects.toMatchObject({ code: "connection_reset" });
    expect(server.numStreams).toBe(0);
  });

  test("aborting the writable resets the stream", async () => {
    const { client, server } = pair();
    const c = await client.open();
    const s = await server.accept();
    await c.writable.abort();
    await tick();
    await expect(s.readable.getReader().read()).rejects.toMatchObject({ code: "connection_reset" });
  });

  test("a cancelled reader keeps granting window, so the peer can finish", async () => {
    const { client, server } = pair({ server: { maxStreamWindow: 256 * 1024 } });
    const c = await client.open();
    const s = await server.accept();
    await s.readable.cancel();
    await writeAll(c, randomBytes(2 * 1024 * 1024)); // would hang without window updates
  });
});

describe("session", () => {
  test("close fails pending reads, accepts and opens", async () => {
    const { client, server } = pair();
    const c = await client.open();
    const read = c.readable.getReader().read().catch((e) => e);
    const accepting = client.accept().catch((e) => e);
    await server.accept();
    client.close();
    expect(await read).toMatchObject({ code: "session_shutdown" });
    expect(await accepting).toMatchObject({ code: "session_shutdown" });
    await expect(client.open()).rejects.toMatchObject({ code: "session_shutdown" });
    expect((await server.closed).message).toBe("transport closed");
  });

  test("a stream the peer had finished keeps its data through session close", async () => {
    const { client, server } = pair();
    const c = await client.open();
    const s = await server.accept();
    await writeAll(s, text("all of it"));
    await tick();
    client.close();
    expect(str(await readAll(c.readable))).toBe("all of it");
  });

  test("ping", async () => {
    const { client, server } = pair();
    expect(await client.ping()).toBeGreaterThanOrEqual(0);
    expect(await server.ping()).toBeGreaterThanOrEqual(0);
  });

  test("go away: peer cannot open, existing streams carry on", async () => {
    const { client, server } = pair();
    const c = await client.open();
    const s = await server.accept();
    server.goAway();
    await tick();
    await expect(client.open()).rejects.toMatchObject({ code: "remote_go_away" });
    await writeAll(c, text("still"));
    expect(str(await readAll(s.readable))).toBe("still");
  });

  test("open times out without ACK and takes the session down", async () => {
    const sent: Uint8Array[] = [];
    const s = new Session({ send: (d) => sent.push(d), close: () => {} }, { openTimeoutMs: 20 });
    await s.open();
    const reason = await s.closed;
    expect(reason).toBeInstanceOf(YamuxError);
    expect((reason as YamuxError).code).toBe("timeout");
  });

  test("byte-at-a-time delivery", async () => {
    const { client, server } = pair({ split: 1 });
    const c = await client.open();
    const serverSide = (async () => {
      const s = await server.accept();
      await writeAll(s, await readAll(s.readable));
    })();
    const data = randomBytes(5000);
    const [, got] = await Promise.all([writeAll(c, data, 777), readAll(c.readable)]);
    expect(Buffer.compare(got, data)).toBe(0);
    await serverSide;
  });
});

describe("wire", () => {
  // A session fed raw frames, with everything it sends captured.
  function raw(opts = {}) {
    const sent: Uint8Array[] = [];
    let closed = false;
    const s = new Session(
      {
        send: (d) => sent.push(d.slice()),
        close: () => (closed = true),
      },
      opts,
    );
    const frames = () =>
      sent.map((f) => {
        const v = new DataView(f.buffer);
        return { type: f[1], flags: v.getUint16(2), id: v.getUint32(4), length: v.getUint32(8) };
      });
    return { s, frames, isTransportClosed: () => closed };
  }

  test("open sends a window update with SYN topping the window up to max", async () => {
    const { s, frames } = raw({ client: true, maxStreamWindow: 1024 * 1024 });
    await s.open();
    expect(frames()).toEqual([{ type: 1, flags: 1, id: 1, length: 1024 * 1024 - 256 * 1024 }]);
  });

  test("accept sends ACK", async () => {
    const { s, frames } = raw({ maxStreamWindow: 256 * 1024 });
    s.receive(rawFrame(1, 1, 1, 0));
    await s.accept();
    expect(frames()).toEqual([{ type: 1, flags: 2, id: 1, length: 0 }]);
  });

  test("answers ping with the same opaque value", () => {
    const { s, frames } = raw();
    s.receive(rawFrame(2, 1, 0, 0xdeadbeef));
    expect(frames()).toEqual([{ type: 2, flags: 2, id: 0, length: 0xdeadbeef }]);
  });

  test("bad version: go away with protocol error, session closed", async () => {
    const { s, frames, isTransportClosed } = raw();
    const bad = rawFrame(0, 0, 1, 0);
    bad[0] = 1;
    s.receive(bad);
    expect(frames()).toEqual([{ type: 3, flags: 0, id: 0, length: 1 }]);
    expect(isTransportClosed()).toBe(true);
    expect(((await s.closed) as YamuxError).code).toBe("invalid_version");
  });

  test("bad message type", async () => {
    const { s } = raw();
    s.receive(rawFrame(9, 0, 0, 0));
    expect(((await s.closed) as YamuxError).code).toBe("invalid_msg_type");
  });

  test("data past the window is a protocol error", async () => {
    const { s } = raw();
    s.receive(rawFrame(1, 1, 1, 0));
    s.receive(rawFrame(0, 0, 1, 256 * 1024 + 1));
    expect(((await s.closed) as YamuxError).code).toBe("recv_window_exceeded");
  });

  test("duplicate SYN is a protocol error", async () => {
    const { s } = raw();
    s.receive(rawFrame(1, 1, 1, 0));
    s.receive(rawFrame(1, 1, 1, 0));
    expect(((await s.closed) as YamuxError).code).toBe("duplicate_stream");
  });

  test("data for an unknown stream is drained, not fatal", async () => {
    const { s } = raw();
    s.receive(rawFrame(0, 0, 99, 5, text("abcde")));
    s.receive(rawFrame(0, 1, 1, 2, text("hi")));
    const st = await s.accept();
    const r = st.readable.getReader();
    expect(str((await r.read()).value!)).toBe("hi");
    expect(s.isClosed).toBe(false);
  });

  test("backlog full: the extra SYN is reset, the session lives", () => {
    const { s, frames } = raw({ acceptBacklog: 2 });
    for (const id of [1, 3, 5]) s.receive(rawFrame(1, 1, id, 0));
    expect(frames()).toEqual([{ type: 1, flags: 8, id: 5, length: 0 }]);
    expect(s.isClosed).toBe(false);
  });

  test("after local go away, incoming SYN is reset", () => {
    const { s, frames } = raw();
    s.goAway();
    s.receive(rawFrame(1, 1, 1, 0));
    expect(frames()).toEqual([
      { type: 3, flags: 0, id: 0, length: 0 },
      { type: 1, flags: 8, id: 1, length: 0 },
    ]);
  });

  test("FIN carried on the data frame arrives after the data", async () => {
    const { s } = raw();
    s.receive(rawFrame(1, 1, 1, 0));
    const st = await s.accept();
    const frame = rawFrame(0, 4, 1, 3, text("end"));
    s.receive(frame.subarray(0, 13)); // header + 1 byte
    s.receive(frame.subarray(13));
    expect(str(await readAll(st.readable))).toBe("end");
  });

  test("data arriving for a stream reset mid-payload is dropped quietly", async () => {
    const { s } = raw();
    s.receive(rawFrame(1, 1, 1, 0));
    const st = await s.accept();
    const frame = rawFrame(0, 4, 1, 3, text("end"));
    s.receive(frame.subarray(0, 13));
    st.reset();
    s.receive(frame.subarray(13));
    expect(s.isClosed).toBe(false);
  });
});
