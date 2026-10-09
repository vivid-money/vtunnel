// The tunnel protocol above yamux: every stream
// opens with one length-prefixed JSON header saying what it is for. A tunnel
// stream then carries bytes and nothing else; a control stream reads one reply
// back and closes.

import { ByteQueue } from "./bytes.ts";

export const STREAM_TUNNEL = "tunnel"; // bytes for a forwarded port
export const STREAM_LISTEN = "listen"; // ask the sandbox to open a port
export const STREAM_POLICY = "policy"; // tell the sandbox what it may reach directly
export const STREAM_PING = "ping"; // keepalive

/** Bounds a header, so a corrupt or hostile length cannot make us allocate. */
export const MAX_FRAME = 64 * 1024;

/** How long a stream may stay silent after opening before it is dropped. */
export const HEADER_TIMEOUT_MS = 30_000;

/** The header every stream opens with. */
export interface StreamHeader {
  type: string;
  port?: number;
  domains?: string[];
  policy?: WirePolicy;
}

/** An egress policy on the wire: default is a word, so absent never reads as "allow". */
export interface WirePolicy {
  default: string;
  allow?: string[];
  deny?: string[];
}

/** The answer to a control stream. */
export interface StreamReply {
  ok: boolean;
  port?: number;
  error?: string;
  policyApplied?: boolean;
}

export class FrameError extends Error {
  override name = "FrameError";
}

/** Encodes v as one frame: 4 bytes of big-endian length, then JSON. */
export function encodeFrame(v: unknown): Uint8Array {
  const body = new TextEncoder().encode(JSON.stringify(v));
  if (body.length > MAX_FRAME) throw new FrameError(`frame too large (${body.length} bytes)`);
  const out = new Uint8Array(4 + body.length);
  new DataView(out.buffer).setUint32(0, body.length);
  out.set(body, 4);
  return out;
}

/** Writes one frame to w and leaves it open. */
export async function writeFrame(w: WritableStreamDefaultWriter<Uint8Array>, v: unknown): Promise<void> {
  await w.write(encodeFrame(v));
}

/** The part of a ReadableStreamDefaultReader that FrameReader uses. */
export interface ByteReader {
  read(): Promise<{ done: true; value?: Uint8Array } | { done: false; value: Uint8Array }>;
  cancel(reason?: unknown): Promise<void>;
}

/**
 * FrameReader reads frames off a stream that delivers bytes in arbitrary
 * pieces, and keeps whatever arrived past the last frame for rest().
 */
export class FrameReader {
  private readonly buf = new ByteQueue();
  private eof = false;

  constructor(private readonly reader: ByteReader) {}

  /**
   * Reads one frame. With timeoutMs, gives up after that long; the stream is
   * not touched, so the caller decides what to do with a silent peer.
   */
  async read<T = unknown>(timeoutMs?: number): Promise<T> {
    const work = this.readFrame<T>();
    if (timeoutMs === undefined) return work;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new FrameError(`no frame within ${timeoutMs} ms`)), timeoutMs);
    });
    try {
      return await Promise.race([work, timeout]);
    } finally {
      if (timer !== undefined) clearTimeout(timer);
    }
  }

  /** Everything after the frames read so far, as a stream. */
  rest(): ReadableStream<Uint8Array> {
    const leftover = this.buf.drain();
    const reader = this.reader;
    const eof = this.eof;
    return new ReadableStream<Uint8Array>(
      {
        async pull(controller) {
          const next = leftover.shift();
          if (next) return controller.enqueue(next);
          if (eof) return controller.close();
          const { value, done } = await reader.read();
          if (done) controller.close();
          else controller.enqueue(value);
        },
        cancel: (reason) => reader.cancel(reason),
      },
      { highWaterMark: 0 },
    );
  }

  private async readFrame<T>(): Promise<T> {
    await this.fill(4);
    const n = new DataView(this.buf.read(4).slice().buffer).getUint32(0);
    if (n > MAX_FRAME) throw new FrameError(`frame too large (${n} bytes)`);
    await this.fill(n);
    const body = this.buf.read(n);
    try {
      return JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(body)) as T;
    } catch (e) {
      throw new FrameError(`bad frame: ${e instanceof Error ? e.message : e}`);
    }
  }

  private async fill(n: number): Promise<void> {
    while (this.buf.length < n) {
      if (this.eof) throw new FrameError("stream ended mid-frame");
      const { value, done } = await this.reader.read();
      if (done) this.eof = true;
      else this.buf.push(value);
    }
  }
}
