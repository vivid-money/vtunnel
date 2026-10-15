// Real controllers for the compatibility tests: testdata/vtclient processes —
// vtunnel.Client with a MITM proxy — dialling a TunnelServer served here.

import type { Subprocess } from "bun";
import { TunnelEgress } from "../src/egress.ts";
import { TunnelServer } from "../src/server.ts";
import type { SessionOptions } from "../src/yamux.ts";
import { str, tick } from "./helpers.ts";
import { buildGoHelper, serveSessions, type WSServer } from "./wsserver.ts";

let built: { bin: string; cleanup: () => void } | undefined;

/** Builds testdata/vtclient once per test file. */
export function buildController(): void {
  built ??= buildGoHelper("vtclient");
}

/** The built testdata/vtclient binary. */
export function controllerBin(): string {
  if (!built) throw new Error("buildController() first");
  return built.bin;
}

export function removeController(): void {
  built?.cleanup();
  built = undefined;
}

export class Harness {
  readonly server = new TunnelServer({ log: () => {} });
  readonly egress = new TunnelEgress(this.server);
  readonly ws: WSServer;
  readonly procs: Subprocess<"ignore", "pipe", "pipe">[] = [];
  private readonly stderr: Promise<string>[] = [];

  constructor(session: SessionOptions = {}) {
    this.ws = serveSessions((s) => void this.server.attach(s, `pod-${this.ws.sessions.length}`), session);
  }

  /** Starts n controllers and waits until each is set up and routed. */
  async start(n = 1): Promise<this> {
    if (!built) throw new Error("buildController() first");
    const ready: Promise<boolean>[] = [];
    for (let i = 0; i < n; i++) {
      const p = Bun.spawn([built.bin, "-url", this.ws.url], { stdout: "pipe", stderr: "pipe" });
      this.procs.push(p);
      this.stderr.push(new Response(p.stderr).text());
      ready.push(readyLine(p.stdout));
    }
    // "ready" means routes are declared and the policy answered: pulling the
    // connection before that fails the controller's setup, not the thing tested.
    if (!(await Promise.all(ready)).every(Boolean)) throw new Error(`controller died: ${await this.logs()}`);
    await this.until(() => this.server.sessions === this.procs.length && this.server.routes("fwd.test:80"));
    return this;
  }

  async until(cond: () => boolean, ms = 10_000): Promise<void> {
    const deadline = Date.now() + ms;
    while (!cond()) {
      if (Date.now() > deadline) {
        const state = `sessions=${this.server.sessions} connections=${this.ws.sessions.length}`;
        throw new Error(`condition not met in ${ms} ms (${state}); controllers said:\n${await this.logs()}`);
      }
      await tick(10);
    }
  }

  stop(): void {
    for (const p of this.procs) p.kill();
    this.server.close();
    this.ws.stop();
  }

  private async logs(): Promise<string> {
    for (const p of this.procs) p.kill();
    return (await Promise.all(this.stderr)).join("\n");
  }
}

async function readyLine(stdout: ReadableStream<Uint8Array>): Promise<boolean> {
  let seen = "";
  for await (const chunk of stdout) {
    seen += str(chunk);
    if (seen.includes("ready\n")) return true;
  }
  return false;
}
