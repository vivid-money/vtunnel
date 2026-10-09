// The sandbox end of vtunnel, for a Durable Object.
//
// A controller dials in exactly as it dials a sandbox and sees nothing
// different: it asks for a port with the domains it serves (listen), may send
// an egress policy (policy), pings, and has tunnel streams opened to it. What
// changes is what sits on this side of those ports. A sandbox has an egress
// proxy listening on real ones; here a port is only a routing key, and the
// caller is code in this isolate asking for a host — openTunnel.
//
// Two things differ from a sandbox:
//
//   - Many controllers at once. A sandbox serves one client at a time; here
//     every controller pod holds its own session,
//     each with its own routes, and a host is served by any session that
//     routes it.
//   - No direct egress, ever. A host no session routes is refused, never
//     dialled. A policy is therefore accepted only if it asks for exactly that.

import {
  encodeFrame,
  FrameReader,
  HEADER_TIMEOUT_MS,
  STREAM_LISTEN,
  STREAM_PING,
  STREAM_POLICY,
  STREAM_TUNNEL,
  type StreamHeader,
  type StreamReply,
} from "./frame.ts";
import { bestDomainMatch, canonicalHostPort } from "./routes.ts";
import { YamuxError, type Session, type Stream } from "./yamux.ts";

export interface ServerOptions {
  /** How often each session is pinged; three missed intervals close it. Default 30 s. 0 disables. */
  keepAliveMs?: number;
  /** How long an accepted stream may stay silent before its header arrives. Default 30 s. */
  headerTimeoutMs?: number;
  log?: (message: string) => void;
}

/** No session routes the host asked for. Nothing was sent anywhere. */
export class NoRouteError extends Error {
  override name = "NoRouteError";
}

/** One connected controller. */
interface Peer {
  readonly session: Session;
  readonly label: string;
  /** port → domains, as the controller last declared them. */
  readonly ports: Map<number, string[]>;
  /** domain → port, the union of ports; rebuilt on every listen. */
  index: Map<string, number>;
}

export interface Tunnel {
  stream: Stream;
  /** The tunnel port the controller allocated for the matching domain. */
  port: number;
  /** The route pattern that matched. */
  pattern: string;
  /** Which session carries it, as given to attach. */
  session: string;
}

// Where allocated ports start: the IANA dynamic range, as the ephemeral ports a
// sandbox hands out come from. The number means nothing here but must be
// unique per session, and looking like a port keeps controller logs familiar.
const FIRST_PORT = 49152;

export class TunnelServer {
  private readonly peers = new Set<Peer>();
  private readonly keepAliveMs: number;
  private readonly headerTimeoutMs: number;
  private readonly log: (message: string) => void;
  private nextPort = FIRST_PORT;
  private turn = 0;
  private closed = false;

  constructor(opts: ServerOptions = {}) {
    this.keepAliveMs = opts.keepAliveMs ?? 30_000;
    this.headerTimeoutMs = opts.headerTimeoutMs ?? HEADER_TIMEOUT_MS;
    this.log = opts.log ?? ((m) => console.log(`[vtunnel-do] ${m}`));
  }

  /** How many controllers are connected. */
  get sessions(): number {
    return this.peers.size;
  }

  /**
   * Serves a controller's session until it ends. label names it in logs and in
   * openTunnel's answer.
   */
  async attach(session: Session, label = "controller"): Promise<void> {
    if (this.closed) {
      session.close();
      return;
    }
    const peer: Peer = { session, label, ports: new Map(), index: new Map() };
    this.peers.add(peer);
    this.log(`${label}: connected (${this.peers.size} now)`);
    const keepAlive = this.keepAliveMs > 0 ? this.keepAlive(peer) : undefined;
    try {
      for await (const stream of session) void this.handleStream(peer, stream);
    } finally {
      this.peers.delete(peer);
      session.close();
      await keepAlive;
      this.log(`${label}: disconnected (${this.peers.size} left)`);
    }
  }

  /** Whether some session routes hostPort right now. */
  routes(hostPort: string): boolean {
    return this.candidates(hostPort).length > 0;
  }

  /**
   * Opens a tunnel stream for hostPort ("host:port") on a session that routes
   * it, taking turns among those that do. What goes on the stream is the
   * caller's: the controller's proxy reads it as a proxy client's connection —
   * a CONNECT, or a request in absolute form.
   */
  async openTunnel(hostPort: string): Promise<Tunnel> {
    const candidates = this.candidates(hostPort);
    if (candidates.length === 0) throw new NoRouteError(`no route for ${hostPort}`);
    const { peer, port, pattern } = candidates[this.turn++ % candidates.length]!;

    const stream = await peer.session.open();
    const writer = stream.writable.getWriter();
    try {
      await writer.write(encodeFrame({ type: STREAM_TUNNEL, port } satisfies StreamHeader));
    } catch (e) {
      stream.reset();
      throw e;
    } finally {
      writer.releaseLock();
    }
    return { stream, port, pattern, session: peer.label };
  }

  /** Closes every session; refuses new ones. */
  close(): void {
    this.closed = true;
    for (const peer of this.peers) peer.session.close();
  }

  private candidates(hostPort: string): { peer: Peer; port: number; pattern: string }[] {
    if (canonicalHostPort(hostPort) === null) return [];
    const out = [];
    for (const peer of this.peers) {
      if (peer.session.isClosed) continue;
      const pattern = bestDomainMatch(peer.index.keys(), hostPort);
      if (pattern !== null) out.push({ peer, port: peer.index.get(pattern)!, pattern });
    }
    return out;
  }

  private async handleStream(peer: Peer, stream: Stream): Promise<void> {
    const reader = stream.readable.getReader();
    let header: StreamHeader;
    try {
      header = await new FrameReader(reader).read<StreamHeader>(this.headerTimeoutMs);
      if (typeof header !== "object" || header === null || typeof header.type !== "string") {
        throw new Error("header has no type");
      }
    } catch (e) {
      this.log(`${peer.label}: dropping a stream: ${e instanceof Error ? e.message : e}`);
      stream.reset();
      return;
    }

    let reply: StreamReply;
    switch (header.type) {
      case STREAM_PING:
        reply = { ok: true };
        break;
      case STREAM_LISTEN:
        reply = this.handleListen(peer, header);
        break;
      case STREAM_POLICY:
        reply = this.handlePolicy(peer, header);
        break;
      default:
        this.log(`${peer.label}: unknown stream type ${JSON.stringify(header.type)}`);
        reply = { ok: false, error: "unknown stream type" };
    }

    // One reply, then FIN. Whatever the peer still sends is drained, not read.
    const writer = stream.writable.getWriter();
    try {
      await writer.write(encodeFrame(reply));
      await writer.close();
    } catch {
      stream.reset();
    }
    void reader.cancel().catch(() => {});
  }

  // A port here is only a name. Port 0 asks
  // for one to be allocated; a port the controller names — the one it was
  // given before, replayed after a reconnect — is taken as is. Either way the
  // port's domains are replaced wholesale, including down to none, which is
  // how a controller withdraws its last route.
  //
  // Routes are installed before the answer goes out: the answer is
  // what tells the controller it is being served.
  private handleListen(peer: Peer, h: StreamHeader): StreamReply {
    const domains = h.domains ?? [];
    if (!Array.isArray(domains) || !domains.every((d) => typeof d === "string")) {
      return { ok: false, error: "domains must be a list of strings" };
    }
    let port = h.port ?? 0;
    if (!Number.isInteger(port) || port < 0 || port > 65535) {
      return { ok: false, error: `bad port ${JSON.stringify(h.port)}` };
    }
    if (port === 0) port = this.allocatePort(peer);

    peer.ports.set(port, [...domains]);
    peer.index = new Map();
    for (const [p, ds] of peer.ports) for (const d of ds) peer.index.set(d, p);
    this.log(`${peer.label}: routes for port ${port}: ${JSON.stringify(domains)}`);
    return { ok: true, port };
  }

  // There is no direct egress here to govern: everything not routed through a
  // tunnel is refused. A policy saying exactly that is applied — it is already
  // true. Anything that allows a direct connection is refused, out loud,
  // because a controller that got OK would believe the sandbox could reach what
  // it cannot.
  private handlePolicy(peer: Peer, h: StreamHeader): StreamReply {
    const p = h.policy;
    if (!p) return { ok: false, error: "policy message carried no policy" };
    if (p.default !== "allow" && p.default !== "deny") {
      return { ok: false, error: `unknown egress default "${p.default}" (want "allow" or "deny")` };
    }
    if (p.default === "allow" || (p.allow?.length ?? 0) > 0) {
      this.log(`${peer.label}: refusing an egress policy that allows direct egress`);
      return { ok: false, error: "this end has no direct egress: only default deny with no allow rules can be applied" };
    }
    return { ok: true, policyApplied: true };
  }

  private allocatePort(peer: Peer): number {
    for (;;) {
      const port = this.nextPort;
      this.nextPort = this.nextPort >= 65535 ? FIRST_PORT : this.nextPort + 1;
      if (!peer.ports.has(port)) return port;
    }
  }

  // A ping unanswered for three intervals means
  // the controller is gone without having said so, and its session is closed
  // so that its routes stop attracting tunnels.
  private async keepAlive(peer: Peer): Promise<void> {
    const { session } = peer;
    while (!session.isClosed) {
      if (await sleepUnless(this.keepAliveMs, session.closed)) return;
      try {
        await withTimeout(this.request(session, { type: STREAM_PING }), 3 * this.keepAliveMs);
      } catch (e) {
        if (session.isClosed) return;
        this.log(`${peer.label}: ping failed (${e instanceof Error ? e.message : e}), closing session`);
        session.close(new YamuxError("timeout", "keepalive failed"));
        return;
      }
    }
  }

  // Opens a control stream, sends h, reads one reply.
  private async request(session: Session, h: StreamHeader): Promise<StreamReply> {
    const stream = await session.open();
    const writer = stream.writable.getWriter();
    try {
      await writer.write(encodeFrame(h));
      await writer.close();
      return await new FrameReader(stream.readable.getReader()).read<StreamReply>(this.headerTimeoutMs);
    } catch (e) {
      stream.reset();
      throw e;
    }
  }
}

/** Resolves true if stop settled first. */
async function sleepUnless(ms: number, stop: Promise<unknown>): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const slept = new Promise<false>((resolve) => (timer = setTimeout(() => resolve(false), ms)));
  try {
    return await Promise.race([slept, stop.then(() => true)]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

async function withTimeout<T>(work: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`no answer within ${ms} ms`)), ms);
  });
  try {
    return await Promise.race([work, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
