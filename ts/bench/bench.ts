// The TypeScript sandbox side under load: cmd/bench, with the server written in
// TypeScript. A Go controller (testdata/vtclient — vtunnel.Client and its MITM
// proxy) dials a TunnelServer running here under Bun; TunnelEgress drives traffic
// through it the way a gateway would.
//
//	bun bench/bench.ts [-size 256MB] [-c 1] [-n 2000] [-mode all] [-window 1MB] [-go]
//
// Modes, comma separated:
//
//	proxy      CONNECT through the tunnel to raw TCP: upload, download, both at
//	           once. cmd/bench's proxy mode, minus the sandbox's TCP hop to its
//	           egress proxy, which this side does not have.
//	fetch      the same through fetch(): an upload body, a download body.
//	stream     open a CONNECT tunnel, write a byte, close: the price of one
//	           tunnelled connection.
//	requests   small fetch() requests, one at a time and 32 at once.
//	handshake  the Go client connecting and disconnecting.
//
// -go also runs cmd/bench over yamux-insecure with the same size, count and
// window, for a Go server to compare against.

import { join } from "node:path";
import type { Duplex } from "../src/egress.ts";
import { buildController, controllerBin, Harness, removeController } from "../test/controller.ts";

const args = parseArgs(process.argv.slice(2));
const size = parseSize(args.size ?? "256MB");
const conns = Number(args.c ?? 1);
const iters = Number(args.n ?? 2000);
const modes = new Set((args.mode ?? "all").split(","));
const window = args.window ? parseSize(args.window) : undefined;
const wants = (m: string) => modes.has(m) || modes.has("all");

const repoRoot = join(import.meta.dir, "..", "..");

console.log(`vtunnel bench, TypeScript server (Bun ${Bun.version}), Go client, yamux-insecure over ws`);
console.log(`  size: ${fmtSize(size)} x ${conns} conn`);
if (window) console.log(`  window: ${fmtSize(window)} per stream (TS receive side)`);
console.log();

let h: Harness;

// ---- modes

async function runProxy() {
  console.log("=== proxy (CONNECT) ===\n");
  await measure("upload", size * conns, () => parallel(conns, () => tcpUpload(size)));
  await measure("download", size * conns, () => parallel(conns, () => tcpDownload(size)));
  await measure("upload+download parallel", 2 * size * conns, () =>
    parallel(conns, () => Promise.all([tcpUpload(size), tcpDownload(size)])),
  );
}

async function runFetch() {
  console.log("=== fetch ===\n");
  await measure("upload (POST body)", size * conns, () => parallel(conns, () => fetchUpload(size)));
  await measure("download (response body)", size * conns, () => parallel(conns, () => fetchDownload(size)));
}

async function runStream() {
  console.log("=== stream open ===\n");
  console.log(`--- open, write, close x${iters} ---`);
  const start = performance.now();
  for (let i = 0; i < iters; i++) {
    const { socket, app } = socketPair();
    const piping = h.egress.connect("tcpsink.test:9", socket);
    await app.writer.write(ONE);
    await app.writer.close();
    await app.readable.cancel();
    await piping;
  }
  printOps(iters, performance.now() - start);
}

async function runRequests() {
  console.log("=== requests (fetch, 204) ===\n");
  for (const c of [1, 32]) {
    console.log(`--- ${iters} requests, ${c} at a time ---`);
    let next = 0;
    const start = performance.now();
    await parallel(c, async () => {
      while (next++ < iters) {
        const res = await h.egress.fetch(new Request("http://api.test/status/204"));
        await res.arrayBuffer();
      }
    });
    printOps(iters, performance.now() - start);
  }
}

async function runHandshake() {
  console.log("=== handshake ===\n");
  const n = Math.max(Math.floor(iters / 20), 20);
  console.log(`--- connect and close x${n} ---`);
  // Not spawnSync: the server under test runs on this event loop, and a
  // blocked loop answers no WebSocket upgrades.
  const proc = Bun.spawn([controllerBin(), "-url", h.ws.url, "-handshakes", String(n)], { stdout: "pipe", stderr: "pipe" });
  const [code, out, err] = await Promise.all([proc.exited, new Response(proc.stdout).text(), new Response(proc.stderr).text()]);
  if (code !== 0) throw new Error(`handshakes: ${err}`);
  const perOpNs = Number(out.trim());
  console.log(`  ${n} ops`);
  console.log(`  ${fmtDuration(perOpNs / 1e6)} per op (${Math.round(1e9 / perOpNs)} ops/s)\n`);
}

function runGo() {
  console.log("\n########## for comparison: cmd/bench, Go server ##########\n");
  const goArgs = ["run", "./cmd/bench", "-protocol", "yamux-insecure", "-mode", "proxy,stream,handshake"];
  goArgs.push("-size", fmtSizeArg(size), "-c", String(conns), "-n", String(iters));
  if (window) goArgs.push("-window", String(window));
  const p = Bun.spawnSync(["go", ...goArgs], { cwd: repoRoot, stdout: "inherit", stderr: "inherit" });
  if (p.exitCode !== 0) console.log(`cmd/bench exited ${p.exitCode}`);
}

// ---- traffic

const CHUNK = new Uint8Array(64 * 1024);
const ONE = Uint8Array.of(1);
let moved = 0;

function socketPair(): { socket: Duplex; app: { writer: WritableStreamDefaultWriter<Uint8Array>; readable: ReadableStream<Uint8Array> } } {
  const toTunnel = new TransformStream<Uint8Array, Uint8Array>();
  const fromTunnel = new TransformStream<Uint8Array, Uint8Array>();
  return {
    socket: { readable: toTunnel.readable, writable: fromTunnel.writable },
    app: { writer: toTunnel.writable.getWriter(), readable: fromTunnel.readable },
  };
}

async function tcpUpload(n: number) {
  const { socket, app } = socketPair();
  const piping = h.egress.connect("tcpsink.test:9", socket);
  for (let sent = 0; sent < n; ) {
    const k = Math.min(CHUNK.length, n - sent);
    await app.writer.write(CHUNK.subarray(0, k));
    sent += k;
    moved += k;
  }
  await app.writer.close();
  await app.readable.cancel();
  await piping;
}

async function tcpDownload(n: number) {
  const { socket, app } = socketPair();
  const piping = h.egress.connect("source.test:9", socket);
  const r = app.readable.getReader();
  for (let got = 0; got < n; ) {
    const { value, done } = await r.read();
    if (done) break;
    got += value.length;
    moved += value.length;
  }
  await r.cancel();
  await app.writer.close();
  await piping.catch(() => {});
}

async function fetchUpload(n: number) {
  let sent = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(c) {
      if (sent >= n) return c.close();
      const k = Math.min(CHUNK.length, n - sent);
      sent += k;
      moved += k;
      c.enqueue(CHUNK.subarray(0, k));
    },
  });
  const res = await h.egress.fetch(
    new Request("http://sink.test/", { method: "POST", body, headers: { "content-length": String(n) }, duplex: "half" } as RequestInit),
  );
  const got = (await res.json()) as { n: number };
  if (got.n !== n) throw new Error(`sink got ${got.n} of ${n}`);
}

async function fetchDownload(n: number) {
  const res = await h.egress.fetch(new Request(`http://api.test/big?n=${n}`));
  for await (const chunk of res.body!) moved += chunk.length;
}

// ---- reporting, in cmd/bench's format

async function measure(name: string, total: number, run: () => Promise<unknown>) {
  console.log(`--- ${name} ---`);
  moved = 0;
  const rss0 = process.memoryUsage().rss;
  let peak = rss0;
  const progress = setInterval(() => {
    peak = Math.max(peak, process.memoryUsage().rss);
    process.stdout.write(`\r  ${fmtSize(moved)} / ${fmtSize(total)}  ${((moved / total) * 100).toFixed(1).padStart(5)}%  `);
  }, 500);
  const start = performance.now();
  await run();
  const ms = performance.now() - start;
  clearInterval(progress);
  peak = Math.max(peak, process.memoryUsage().rss);
  const speed = moved / (ms / 1000);
  process.stdout.write(`\r  ${fmtSize(moved)} in ${fmtDuration(ms)}            \n`);
  let line = `  throughput: ${fmtSize(speed)}/s`;
  const streams = total / size / conns;
  if (conns > 1 || streams > 1) line += ` (${fmtSize(speed / conns / streams)}/s per stream)`;
  console.log(line);
  console.log(`  rss: peak ${fmtSize(peak)} (+${fmtSize(Math.max(0, peak - rss0))} during the run)\n`);
}

function printOps(n: number, ms: number) {
  console.log(`  ${n} ops in ${fmtDuration(ms)}`);
  console.log(`  ${fmtDuration(ms / n)} per op (${Math.round((n / ms) * 1000)} ops/s)\n`);
}

async function parallel(n: number, f: () => Promise<unknown>) {
  await Promise.all(Array.from({ length: n }, f));
}

function parseArgs(argv: string[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]!.replace(/^-+/, "");
    const next = argv[i + 1];
    if (next !== undefined && !next.startsWith("-")) {
      out[a] = next;
      i++;
    } else {
      out[a] = "";
    }
  }
  return out;
}

function parseSize(s: string): number {
  const m = /^([0-9.]+)\s*(TB|GB|MB|KB|B)?$/i.exec(s.trim());
  if (!m) throw new Error(`bad size ${s}`);
  const mult = { TB: 2 ** 40, GB: 2 ** 30, MB: 2 ** 20, KB: 2 ** 10, B: 1 }[(m[2] ?? "B").toUpperCase() as "B"];
  return Math.floor(Number(m[1]) * mult);
}

function fmtSizeArg(b: number): string {
  return b % 2 ** 20 === 0 ? `${b / 2 ** 20}MB` : String(b);
}

function fmtSize(b: number): string {
  if (b >= 2 ** 30) return `${(b / 2 ** 30).toFixed(2)} GB`;
  if (b >= 2 ** 20) return `${(b / 2 ** 20).toFixed(2)} MB`;
  if (b >= 2 ** 10) return `${(b / 2 ** 10).toFixed(2)} KB`;
  return `${Math.round(b)} B`;
}

function fmtDuration(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(3)}s`;
  if (ms >= 1) return `${ms.toFixed(3)}ms`;
  return `${(ms * 1000).toFixed(0)}µs`;
}

async function main() {
  buildController();
  h = await new Harness(window ? { maxStreamWindow: window } : {}).start();
  try {
    if (wants("proxy")) await runProxy();
    if (wants("fetch")) await runFetch();
    if (wants("stream")) await runStream();
    if (wants("requests")) await runRequests();
    if (wants("handshake")) await runHandshake();
  } finally {
    h.stop();
    removeController();
  }
  if (args.go !== undefined) runGo();
}

await main();
