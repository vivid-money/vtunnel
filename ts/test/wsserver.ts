// A WebSocket server for the Go compatibility tests: every connection becomes a
// yamux server session, the way a Durable Object would make one, handed to
// onSession. Also builds the Go helpers those tests run.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ServerWebSocket } from "bun";
import { Session, type SessionOptions } from "../src/yamux.ts";

type Data = { url: string; session?: Session; deaf?: boolean };

export interface WSServer {
  url: string;
  /** Sessions in the order they connected. */
  sessions: Session[];
  /**
   * Makes connection i a half-open one: from now on nothing crosses it either
   * way and its close goes unnoticed, though neither end closes it.
   */
  deafen(i: number): void;
  stop(): void;
}

export function serveSessions(onSession: (s: Session, url: string) => void, opts: SessionOptions = {}): WSServer {
  const sessions: Session[] = [];
  const sockets: ServerWebSocket<Data>[] = [];
  const server = Bun.serve<Data, never>({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req, srv) {
      if (srv.upgrade(req, { data: { url: req.url } })) return;
      return new Response("upgrade required", { status: 426 });
    },
    websocket: {
      open(ws: ServerWebSocket<Data>) {
        const session = new Session(
          {
            send: (d) => {
              if (ws.data.deaf) return;
              if (ws.send(d) === 0) throw new Error("websocket dropped a message");
            },
            close: () => ws.close(),
          },
          opts,
        );
        ws.data.session = session;
        sessions.push(session);
        sockets.push(ws);
        onSession(session, ws.data.url);
      },
      message(ws, message) {
        if (ws.data.deaf) return;
        if (typeof message === "string") {
          ws.data.session!.close(new Error("text message"));
          return;
        }
        ws.data.session!.receive(new Uint8Array(message));
      },
      close(ws) {
        if (ws.data.deaf) return;
        ws.data.session?.transportClosed();
      },
    },
  });
  return {
    url: `ws://127.0.0.1:${server.port}/`,
    sessions,
    deafen: (i) => {
      sockets[i]!.data.deaf = true;
    },
    stop: () => server.stop(true),
  };
}

const repoRoot = join(import.meta.dir, "..", "..");

/** Builds ts/testdata/<name> into a temporary directory; returns the binary and a cleanup. */
export function buildGoHelper(name: string): { bin: string; cleanup: () => void } {
  const dir = mkdtempSync(join(tmpdir(), `${name}-`));
  const bin = join(dir, name);
  const build = Bun.spawnSync(["go", "build", "-o", bin, `./ts/testdata/${name}`], { cwd: repoRoot, stderr: "pipe" });
  if (build.exitCode !== 0) throw new Error(`go build ${name}: ${build.stderr.toString()}`);
  return { bin, cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}
