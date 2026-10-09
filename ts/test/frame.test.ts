import { describe, expect, test } from "bun:test";
import { type ByteReader, encodeFrame, FrameError, FrameReader, MAX_FRAME, type StreamHeader, type StreamReply } from "../src/frame.ts";
import { readAll, str, text } from "./helpers.ts";

// A stream that delivers the given pieces and ends, or stays open if told to.
function source(pieces: Uint8Array[], { end = true } = {}): ByteReader {
  return new ReadableStream<Uint8Array>({
    pull(c) {
      const next = pieces.shift();
      if (next) c.enqueue(next);
      else if (end) c.close();
    },
  }).getReader();
}

function bytes(...parts: (number[] | Uint8Array)[]): Uint8Array {
  return Uint8Array.from(parts.flatMap((p) => [...p]));
}

describe("frame", () => {
  test("encodes as the Go end expects", () => {
    // What writeFrame(w, streamHeader{Type: "tunnel", Port: 8080}) puts on the wire.
    const json = '{"type":"tunnel","port":8080}';
    expect(encodeFrame({ type: "tunnel", port: 8080 } satisfies StreamHeader)).toEqual(
      bytes([0, 0, 0, json.length], text(json)),
    );
  });

  test("reads frames split anywhere, and hands back what follows", async () => {
    const wire = bytes(encodeFrame({ type: "listen", port: 1 }), encodeFrame({ ok: true }), text("tail bytes"));
    const pieces = Array.from(wire, (b) => Uint8Array.of(b));
    const r = new FrameReader(source(pieces));
    expect(await r.read<StreamHeader>()).toEqual({ type: "listen", port: 1 });
    expect(await r.read<StreamReply>()).toEqual({ ok: true });
    expect(str(await readAll(r.rest()))).toBe("tail bytes");
  });

  test("rest after a frame that arrived together with the tail", async () => {
    const r = new FrameReader(source([bytes(encodeFrame({ type: "tunnel" }), text("abc")), text("def")]));
    await r.read();
    expect(str(await readAll(r.rest()))).toBe("abcdef");
  });

  test("64 KiB is the limit, both ways", async () => {
    const atLimit = "x".repeat(MAX_FRAME - 2); // JSON string adds two quotes
    expect(encodeFrame(atLimit).length).toBe(4 + MAX_FRAME);
    expect(() => encodeFrame(atLimit + "x")).toThrow(FrameError);

    const r = new FrameReader(source([bytes([0, 1, 0, 1])]));
    await expect(r.read()).rejects.toThrow("frame too large (65537 bytes)");
  });

  test("stream ending mid-frame", async () => {
    const r = new FrameReader(source([bytes([0, 0, 0, 10], text("{}"))]));
    await expect(r.read()).rejects.toThrow("stream ended mid-frame");
  });

  test("not JSON", async () => {
    const r = new FrameReader(source([bytes([0, 0, 0, 3], text("{x}"))]));
    await expect(r.read()).rejects.toThrow(FrameError);
  });

  test("silent peer times out", async () => {
    const r = new FrameReader(source([], { end: false }));
    await expect(r.read(20)).rejects.toThrow("no frame within 20 ms");
  });
});
