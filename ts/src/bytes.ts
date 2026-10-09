// ByteQueue holds bytes that arrived in arbitrary pieces until a parser wants
// them in the pieces it needs.
//
// A WebSocket says nothing about where one yamux frame ends and the next
// begins — the Go end writes whatever its buffers hold — so every reader of the
// wire has to reassemble. Reading from inside one piece hands back a view of
// it rather than a copy; only a read that straddles pieces allocates.
export class ByteQueue {
  private chunks: Uint8Array[] = [];
  private offset = 0; // into chunks[0]
  private size = 0;

  get length(): number {
    return this.size;
  }

  push(chunk: Uint8Array): void {
    if (chunk.length === 0) return;
    this.chunks.push(chunk);
    this.size += chunk.length;
  }

  /** Removes and returns exactly n bytes. The caller checks length first. */
  read(n: number): Uint8Array {
    if (n > this.size) throw new RangeError(`read ${n} of ${this.size} bytes`);
    this.size -= n;
    const first = this.chunks[0];
    if (n === 0 || first === undefined) return new Uint8Array(0);

    if (first.length - this.offset >= n) {
      const out = first.subarray(this.offset, this.offset + n);
      this.advance(first, n);
      return out;
    }

    const out = new Uint8Array(n);
    let filled = 0;
    while (filled < n) {
      const head = this.chunks[0]!;
      const take = Math.min(head.length - this.offset, n - filled);
      out.set(head.subarray(this.offset, this.offset + take), filled);
      filled += take;
      this.advance(head, take);
    }
    return out;
  }

  /** Position of the first byte equal to b, or -1. */
  indexOf(b: number): number {
    let base = 0;
    for (let i = 0; i < this.chunks.length; i++) {
      const chunk = this.chunks[i]!;
      const from = i === 0 ? this.offset : 0;
      const at = chunk.indexOf(b, from);
      if (at >= 0) return base + at - from;
      base += chunk.length - from;
    }
    return -1;
  }

  /** Removes and returns up to max bytes from the first piece, without copying. */
  take(max: number): Uint8Array {
    const first = this.chunks[0];
    if (first === undefined || max <= 0) return new Uint8Array(0);
    const n = Math.min(max, first.length - this.offset);
    const out = first.subarray(this.offset, this.offset + n);
    this.size -= n;
    this.advance(first, n);
    return out;
  }

  /** Removes and returns everything queued, as the pieces it arrived in. */
  drain(): Uint8Array[] {
    const out = this.chunks;
    if (out.length > 0 && this.offset > 0) out[0] = out[0]!.subarray(this.offset);
    this.chunks = [];
    this.offset = 0;
    this.size = 0;
    return out;
  }

  private advance(head: Uint8Array, n: number): void {
    this.offset += n;
    if (this.offset === head.length) {
      this.chunks.shift();
      this.offset = 0;
    }
  }
}

const IDLE = Symbol("idle");

/**
 * Reads stream and yields what it delivers, joined into pieces of up to max
 * bytes — but only what has already arrived. Once the next read would have to
 * wait, what is held goes out, so an interactive stream is not delayed for the
 * sake of a full piece.
 *
 * It exists because the runtime's pieces are small: a request body reaches a
 * Durable Object over RPC in 4 KiB chunks, and every piece written to the
 * tunnel is a yamux frame and a WebSocket message.
 */
export async function* coalesce(stream: ReadableStream<Uint8Array>, max = 64 * 1024): AsyncGenerator<Uint8Array> {
  const reader = stream.getReader();
  // Stopped before the end, the source is cancelled rather than released: a
  // consumer that stops early wants it stopped, and workerd refuses to release
  // a reader with a read outstanding, which there usually is.
  let finished = false;
  try {
    let next = reader.read();
    for (;;) {
      const first = await next;
      if (first.done) {
        finished = true;
        return;
      }
      if (first.value.length >= max) {
        next = reader.read();
        yield first.value;
        continue;
      }
      const parts = [first.value];
      let size = first.value.length;
      let done = false;
      next = reader.read();
      while (size < max) {
        // A macrotask is long enough for whatever has already arrived to be
        // read, and short enough not to be a delay anyone notices.
        let timer: ReturnType<typeof setTimeout> | undefined;
        const idle = new Promise<typeof IDLE>((res) => (timer = setTimeout(res, 0, IDLE)));
        // Cleared whichever wins: workerd caps a request at 10,000 live
        // timers, and a long body would otherwise leave one per piece behind.
        const r = await Promise.race([next, idle]).finally(() => {
          if (timer !== undefined) clearTimeout(timer);
        });
        if (r === IDLE) break;
        if (r.done) {
          done = true;
          break;
        }
        parts.push(r.value);
        size += r.value.length;
        next = reader.read();
      }
      if (done) finished = true;
      yield join(parts, size);
      if (done) return;
    }
  } finally {
    if (finished) reader.releaseLock();
    else void reader.cancel().catch(() => {});
  }
}

function join(parts: Uint8Array[], size: number): Uint8Array {
  if (parts.length === 1) return parts[0]!;
  const out = new Uint8Array(size);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.length;
  }
  return out;
}
