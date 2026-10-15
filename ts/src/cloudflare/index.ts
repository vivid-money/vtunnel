// Cloudflare bindings for the sandbox side of vtunnel: everything here needs
// cloudflare:workers, and nothing in ../ does.
//
// The shape is a Durable Object holding the tunnel and a WorkerEntrypoint in
// front of it: the Worker routes and checks, and reaches the object through
// typed RPC on its stub — stub.egressFetch(), stub.egressConnect() — never by
// inventing paths for stub.fetch(). stub.fetch() carries exactly one thing:
// the controller's WebSocket upgrade.
//
// A deployment exports two classes from its Worker:
//
//	export class EgressTunnel extends VTunnelObject {}
//	export class Gateway extends EgressGateway<Env> {
//	  tunnel() { return this.env.EGRESS.getByName("egress"); }
//	}
//
// and points a dynamic Worker's globalOutbound at ctx.exports.Gateway(...).
// What happens to a host no controller routes is the deployment's to decide,
// by overriding the gateway's fetch(); see EgressGateway.

export { VTunnelObject } from "./object.ts";
export { EgressGateway } from "./gateway.ts";
export { checkUpgrade } from "./upgrade.ts";
export { hostPortOf } from "../routes.ts";
