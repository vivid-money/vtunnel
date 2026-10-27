import { DurableObject } from "cloudflare:workers";
import { TunnelEgress } from "../egress.ts";
import { TunnelServer } from "../server.ts";
import { UpgradeVerifier } from "../upgrade.ts";
import { PING_TEXT, PONG_TEXT } from "../websocket.ts";
import { Session, YamuxError } from "../yamux.ts";

/**
 * What each controller's WebSocket carries through hibernation: enough to
 * rebuild its session on waking. The session itself — yamux windows, streams —
 * is memory only, and needs to be: the object hibernates only when nothing is
 * in flight, so a rebuilt session has no streams to recover.
 */
interface Attachment {
  id: string;
  label: string;
  /** When the controller connected: its liveness until its first heartbeat. */
  since: number;
  /** port → domains, as the controller last declared them. */
  routes: [number, string[]][];
  /** Stream IDs from here up are unused; a rebuilt session starts here. */
  streamCeiling: number;
}

/**
 * Stream IDs are reserved in blocks, so the attachment is rewritten once per
 * block rather than once per stream. A block wasted on every wake is nothing
 * against 2^31.
 */
const STREAM_BLOCK = 2048;

/**
 * A controller that has not pinged for this long is gone. It pings every 30
 * seconds (vtunnel.WithKeepAlive), and gives up on its own end after 90.
 */
const STALE_MS = 90_000;

/**
 * VTunnelObject is the sandbox end of vtunnel as a Durable Object: controllers
 * dial in over a WebSocket, code in other Workers egresses through it.
 *
 * It hibernates while no call is in flight, so an idle tunnel costs no
 * duration. Controllers keep their connections throughout:
 *
 *   - Their heartbeat ("vtunnel:ping") is answered by the runtime without
 *     waking the object, which also records when each last pinged — how a
 *     vanished controller is told from a quiet one. Whatever does reach the
 *     object — a heartbeat it answers itself, a frame — counts as well.
 *   - Each connection's routes and stream ID ceiling live in its attachment;
 *     on waking, the constructor rebuilds every session from them.
 *   - Nothing here keeps a timer running while idle; one would keep the
 *     object awake.
 *
 * Controllers must send the heartbeat (protocol yamux-wss does). One that does
 * not is taken for gone after staleMs, at the next call.
 */
export class VTunnelObject<Env = unknown> extends DurableObject<Env> {
  /** How long a controller may go without pinging before it is taken for gone. */
  protected readonly staleMs: number = STALE_MS;
  protected readonly server = new TunnelServer({ keepAliveMs: 0 });
  protected readonly egress = new TunnelEgress(this.server);
  private readonly sessions = new Map<string, Session>();
  /**
   * When each controller last sent anything that reached webSocketMessage, by
   * session id: a heartbeat the runtime did not auto-answer, or a frame. Either
   * says the controller is there; the auto-response timestamp sees neither.
   */
  private readonly heard = new Map<string, number>();
  private verifier: UpgradeVerifier | undefined;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair(PING_TEXT, PONG_TEXT));
    // Waking from hibernation: every controller is still connected, and
    // nothing in memory knows it.
    for (const ws of ctx.getWebSockets()) this.resume(ws);
  }

  /**
   * The key controllers sign their upgrade with (protocol yamux-wss): by
   * default the Worker secret VTUNNEL_UPGRADE_KEY. Without one, every
   * controller is refused — there is no unauthenticated mode to fall into.
   */
  protected upgradeKey(): string | undefined {
    return (this.env as { VTUNNEL_UPGRADE_KEY?: string } | undefined)?.VTUNNEL_UPGRADE_KEY;
  }

  /**
   * Takes a controller's WebSocket, if its upgrade is signed. The Worker in
   * front should check the signature too (checkUpgrade) so that nothing
   * unsigned reaches this object at all; this check is the one that also
   * refuses a replayed signature, since every upgrade for the tunnel comes
   * through here.
   */
  override async fetch(request: Request): Promise<Response> {
    if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("vtunnel: WebSocket upgrade required\n", { status: 426 });
    }
    const key = this.upgradeKey();
    if (!key) {
      console.error("[vtunnel-do] refusing a controller: no upgrade key configured (VTUNNEL_UPGRADE_KEY)");
      return new Response("vtunnel: not configured\n", { status: 503 });
    }
    this.verifier ??= new UpgradeVerifier(key);
    const refusal = await this.verifier.verify(request);
    if (refusal) {
      console.error(`[vtunnel-do] refusing a controller: ${refusal}`);
      return new Response(`vtunnel: upgrade refused: ${refusal}\n`, { status: 401 });
    }

    const [client, server] = Object.values(new WebSocketPair()) as [WebSocket, WebSocket];
    this.ctx.acceptWebSocket(server);
    const id = crypto.randomUUID();
    const attachment: Attachment = {
      id,
      label: `controller-${id.slice(0, 8)}`,
      since: Date.now(),
      routes: [],
      streamCeiling: 2,
    };
    server.serializeAttachment(attachment);
    this.resume(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  override async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const session = this.sessionOf(ws);
    if (!session) {
      ws.close(1011, "vtunnel: session lost");
      return;
    }
    this.heard.set((ws.deserializeAttachment() as Attachment).id, Date.now());
    if (typeof message === "string") {
      // A ping that reached the handler is still a ping; anything else is
      // not from a vtunnel peer.
      if (message === PING_TEXT) ws.send(PONG_TEXT);
      else if (message !== PONG_TEXT) session.close(new YamuxError("protocol_error", "non-binary WebSocket message"));
      return;
    }
    session.receive(new Uint8Array(message));
  }

  override async webSocketClose(ws: WebSocket, code: number): Promise<void> {
    this.sessionOf(ws)?.transportClosed();
    try {
      ws.close(code === 1005 ? 1000 : code);
    } catch {
      // already closed
    }
  }

  override async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    this.sessionOf(ws)?.transportClosed(error instanceof Error ? error : new YamuxError("session_shutdown", "WebSocket error"));
  }

  /** Sends request through the tunnel. See TunnelEgress.fetch. */
  async egressFetch(request: Request): Promise<Response> {
    return this.egress.fetch(request);
  }

  /** Pipes a socket's two halves to hostPort through the tunnel. See TunnelEgress.connect. */
  async egressConnect(
    hostPort: string,
    readable: ReadableStream<Uint8Array>,
    writable: WritableStream<Uint8Array>,
  ): Promise<void> {
    return this.egress.connect(hostPort, { readable, writable });
  }

  /** How many controllers are connected. */
  async controllers(): Promise<number> {
    return this.server.sessions;
  }

  /** How many tunnel streams are open. */
  async openStreams(): Promise<number> {
    return this.server.streams;
  }

  /** Whether some controller routes hostPort. */
  async routes(hostPort: string): Promise<boolean> {
    return this.server.routes(hostPort);
  }

  // Builds the session for a connection, new or carried through hibernation,
  // and serves it until the connection ends.
  private resume(ws: WebSocket): void {
    const state = ws.deserializeAttachment() as Attachment | null;
    if (!state) {
      ws.close(1011, "vtunnel: session lost");
      return;
    }
    const save = () => ws.serializeAttachment(state);
    const session = new Session(
      {
        send: (data) => ws.send(data),
        close: () => {
          try {
            ws.close(1000);
          } catch {
            // already closing
          }
        },
      },
      {
        firstStreamId: state.streamCeiling,
        onOpen: (id) => {
          if (id < state.streamCeiling) return;
          state.streamCeiling = id + STREAM_BLOCK;
          save();
        },
      },
    );
    this.sessions.set(state.id, session);
    void this.server
      .attach(session, state.label, {
        routes: state.routes,
        onRoutes: (routes) => {
          state.routes = routes;
          save();
        },
        alive: () => {
          const last = Math.max(
            this.ctx.getWebSocketAutoResponseTimestamp(ws)?.getTime() ?? state.since,
            this.heard.get(state.id) ?? 0,
          );
          return Date.now() - last <= this.staleMs;
        },
      })
      .finally(() => {
        this.sessions.delete(state.id);
        this.heard.delete(state.id);
      });
  }

  private sessionOf(ws: WebSocket): Session | undefined {
    const state = ws.deserializeAttachment() as Attachment | null;
    return state ? this.sessions.get(state.id) : undefined;
  }
}
