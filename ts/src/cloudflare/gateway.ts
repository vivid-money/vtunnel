import { WorkerEntrypoint } from "cloudflare:workers";
import type { VTunnelObject } from "./object.ts";

/** The part of a VTunnelObject stub a gateway calls. */
export type TunnelStub = Pick<DurableObjectStub<VTunnelObject>, "egressFetch" | "egressConnect" | "routes">;

/**
 * EgressGateway is what a dynamic Worker's globalOutbound points at: every
 * fetch() and connect() it makes arrives here instead of at the network, and
 * goes on to the tunnel object. tunnel() says which object.
 *
 * It sends everything to the tunnel, and a host no controller routes gets the
 * object's answer to that: 502, x-vtunnel-error: no-route. Anything else is
 * a decision for the deployment, made in an override:
 *
 *	override async fetch(request: Request) {
 *	  if (await this.tunnel().routes(hostPortOf(request.url))) return super.fetch(request);
 *	  return fetch(request); // or refuse, or log
 *	}
 *
 * A fetch arrives as a parsed Request, before any TLS — the runtime has not
 * dialled anything yet — so it crosses the tunnel as HTTP and the controller
 * makes the real connection. A connect arrives as a socket whose destination
 * is in opened.localAddress, and is piped. connect() with secureTransport
 * "on" or "starttls" never gets here: workerd refuses to deliver TLS to a
 * connect handler ("Incoming CONNECT with TLS not supported").
 */
export abstract class EgressGateway<Env = unknown, Props = unknown> extends WorkerEntrypoint<Env, Props> {
  /** The tunnel object to egress through. */
  protected abstract tunnel(): TunnelStub;

  override async fetch(request: Request): Promise<Response> {
    const response = await this.tunnel().egressFetch(request);
    return enforceLength(response);
  }

  override async connect(socket: Socket): Promise<void> {
    const { localAddress } = await socket.opened;
    if (!localAddress) {
      await socket.close();
      return;
    }
    try {
      await this.tunnel().egressConnect(localAddress, socket.readable, socket.writable);
    } catch {
      // No route, or the controller refused. The application sees its socket
      // end without a byte — and only that: workerd hands an application
      // nothing but EOF whatever a connect handler does, whether it throws,
      // aborts the writable or cancels the readable.
      await socket.close().catch(() => {});
    }
  }
}

/**
 * Makes a body of known length fail, rather than end, if it is cut short.
 *
 * A body that errors on its way from the tunnel object to here still errors
 * (RPC carries that). From here on to the application it does not: workerd
 * ends the application's body cleanly, and a response truncated by a dying
 * controller reads as a complete one. A FixedLengthStream is the one thing
 * workerd checks — fall short of it and the application's read fails with
 * "Network connection lost". A body of unknown
 * length has nothing to check against, and a truncation there still reads as
 * the end.
 */
function enforceLength(response: Response): Response {
  const declared = response.headers.get("content-length");
  if (response.body === null || declared === null || !/^[0-9]+$/.test(declared)) return response;
  const fixed = new FixedLengthStream(Number(declared));
  response.body.pipeTo(fixed.writable).catch(() => {});
  return new Response(fixed.readable, response);
}
