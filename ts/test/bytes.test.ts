import { afterEach, describe, expect, test } from "vitest";
import { ByteQueue, coalesce } from "../src/bytes.ts";

describe("ByteQueue", () => {
  test("indexOf and take across pieces", () => {
    const q = new ByteQueue();
    q.push(Uint8Array.of(1, 2));
    q.push(Uint8Array.of(3, 10, 5));
    expect(q.indexOf(10)).toBe(3);
    expect([...q.take(5)]).toEqual([1, 2]);
    expect(q.indexOf(10)).toBe(1);
    expect(q.indexOf(99)).toBe(-1);
    expect([...q.read(3)]).toEqual([3, 10, 5]);
  });
});

describe("coalesce", () => {
  const realSet = globalThis.setTimeout;
  const realClear = globalThis.clearTimeout;
  afterEach(() => {
    globalThis.setTimeout = realSet;
    globalThis.clearTimeout = realClear;
  });

  function source(pieces: number, size: number, { gapEvery = 0 } = {}) {
    let i = 0;
    return new ReadableStream<Uint8Array>({
      async pull(c) {
        if (i >= pieces) return c.close();
        if (gapEvery && i % gapEvery === 0 && i > 0) await new Promise((r) => realSet(r, 5));
        c.enqueue(new Uint8Array(size).fill(i++ % 256));
      },
    });
  }

  test("joins what has arrived into pieces of up to max, losing nothing", async () => {
    const out: Uint8Array[] = [];
    for await (const p of coalesce(source(100, 4096), 64 * 1024)) out.push(p);
    expect(out.reduce((n, p) => n + p.length, 0)).toBe(100 * 4096);
    expect(out.length).toBeLessThan(20);
    expect(Math.max(...out.map((p) => p.length))).toBeLessThanOrEqual(64 * 1024 + 4096);
  });

  test("does not hold data back waiting for a full piece", async () => {
    const out: number[] = [];
    for await (const p of coalesce(source(9, 10, { gapEvery: 3 }))) out.push(p.length);
    expect(out).toEqual([30, 30, 30]);
  });

  // workerd caps a request at 10,000 live timers; leaving one behind per
  // piece failed every long upload there.
  test("leaves no timer behind", async () => {
    let live = 0;
    let peak = 0;
    globalThis.setTimeout = ((fn: () => void, ms?: number, ...args: unknown[]) => {
      live++;
      peak = Math.max(peak, live);
      return realSet(() => {
        live--;
        (fn as (...a: unknown[]) => void)(...args);
      }, ms);
    }) as typeof setTimeout;
    globalThis.clearTimeout = ((t: ReturnType<typeof setTimeout>) => {
      live--;
      realClear(t);
    }) as typeof clearTimeout;
    let n = 0;
    for await (const p of coalesce(source(20_000, 64), 64 * 1024)) n += p.length;
    expect(n).toBe(20_000 * 64);
    expect(peak).toBeLessThan(10);
  });

  test("a consumer that stops early stops the source", async () => {
    let cancelled = false;
    const s = new ReadableStream<Uint8Array>({
      pull: (c) => c.enqueue(new Uint8Array(10)),
      cancel: () => void (cancelled = true),
    });
    for await (const _ of coalesce(s)) break;
    await new Promise((r) => realSet(r, 10));
    expect(cancelled).toBe(true);
  });
});
