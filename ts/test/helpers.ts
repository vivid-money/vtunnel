import { Session, type SessionOptions, type Stream } from "../src/yamux.ts";

export interface Pair {
  client: Session;
  server: Session;
  /** Bytes the client sent, as frames went out. */
  clientSent: Uint8Array[];
}

/**
 * Two sessions wired back to back. Delivery is a microtask later, never
 * re-entrant, which is how a WebSocket behaves. split cuts every send into
 * pieces of that many bytes, to exercise reassembly.
 */
export function pair(opts: { client?: SessionOptions; server?: SessionOptions; split?: number } = {}): Pair {
  const clientSent: Uint8Array[] = [];
  let client!: Session;
  let server!: Session;
  const deliver = (to: () => Session, data: Uint8Array) => {
    const copy = data.slice();
    const pieces: Uint8Array[] = [];
    const step = opts.split ?? copy.length;
    for (let i = 0; i < copy.length; i += step) pieces.push(copy.subarray(i, i + step));
    queueMicrotask(() => {
      for (const piece of pieces) to().receive(piece);
    });
  };
  client = new Session(
    {
      send: (d) => {
        clientSent.push(d.slice());
        deliver(() => server, d);
      },
      close: () => queueMicrotask(() => server.transportClosed()),
    },
    { ...opts.client, client: true },
  );
  server = new Session(
    {
      send: (d) => deliver(() => client, d),
      close: () => queueMicrotask(() => client.transportClosed()),
    },
    { ...opts.server, client: false },
  );
  return { client, server, clientSent };
}

export async function readAll(stream: ReadableStream<Uint8Array>): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let n = 0;
  for await (const part of stream) {
    parts.push(part);
    n += part.length;
  }
  const out = new Uint8Array(n);
  let off = 0;
  for (const part of parts) {
    out.set(part, off);
    off += part.length;
  }
  return out;
}

export async function writeAll(stream: Stream, data: Uint8Array, chunk = 100_000): Promise<void> {
  const w = stream.writable.getWriter();
  for (let i = 0; i < data.length; i += chunk) await w.write(data.subarray(i, i + chunk));
  await w.close();
}

export function randomBytes(n: number): Uint8Array {
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i += 65536) crypto.getRandomValues(out.subarray(i, Math.min(n, i + 65536)));
  return out;
}

export const text = (s: string) => new TextEncoder().encode(s);
export const str = (b: Uint8Array) => new TextDecoder().decode(b);

export function tick(ms = 0): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** A raw yamux frame, for feeding a session things a well-behaved peer would not send. */
export function rawFrame(type: number, flags: number, id: number, length: number, body?: Uint8Array): Uint8Array {
  const out = new Uint8Array(12 + (body?.length ?? 0));
  const v = new DataView(out.buffer);
  out[0] = 0;
  out[1] = type;
  v.setUint16(2, flags);
  v.setUint32(4, id);
  v.setUint32(8, length);
  if (body) out.set(body, 12);
  return out;
}
