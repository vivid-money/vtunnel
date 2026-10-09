import { DurableObject } from "cloudflare:workers";
import { Egress } from "../egress.ts";
import { TunnelServer } from "../server.ts";
import { UpgradeVerifier } from "../upgrade.ts";
import { sessionOverWebSocket } from "../websocket.ts";

/**
 * VTunnelObject is the sandbox end of vtunnel as a Durable Object: controllers
 * dial in over a WebSocket, code in other Workers egresses through it.
 *
 * It must not hibernate. Every session's yamux state — windows, streams,
 * buffers — lives in this object's memory, and the hibernation API evicts
 * the object while its sockets stay up, which would leave the controller
 * talking to a session that no longer exists. So it accepts its WebSockets
 * the ordinary way, and stays resident for as long as a controller is
 * connected. If it is evicted anyway — a deploy, a restart — the sockets
 * close with it, controllers reconnect, and calls in flight fail as network
 * errors.
 */
export class VTunnelObject<Env = unknown> extends DurableObject<Env> {
  protected readonly server = new TunnelServer();
  protected readonly egress = new Egress(this.server);
  private connections = 0;

  private verifier: UpgradeVerifier | undefined;

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
    server.accept();
    const session = sessionOverWebSocket(server);
    void this.server.attach(session, `controller-${++this.connections}`);
    return new Response(null, { status: 101, webSocket: client });
  }

  /** Sends request through the tunnel. See Egress.fetch. */
  async egressFetch(request: Request): Promise<Response> {
    return this.egress.fetch(request);
  }

  /** Pipes a socket's two halves to hostPort through the tunnel. See Egress.connect. */
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

  /** Whether some controller routes hostPort. */
  async routes(hostPort: string): Promise<boolean> {
    return this.server.routes(hostPort);
  }
}
