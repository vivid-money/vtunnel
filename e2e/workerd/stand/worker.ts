// The stand's one Worker, laid out as a deployment would lay it out:
//
//   - EgressTunnel: the Durable Object holding the tunnel (VTunnelObject).
//   - Egress: the gateway a dynamic Worker's globalOutbound points at.
//   - default: the public face. /tunnel checks a controller's signed
//     WebSocket upgrade (yamux-wss) and passes it to the object; /run loads
//     the test application as a dynamic Worker, egressing through Egress, and
//     has it run one case; /status reports.
//
// Built by the Go test with bun build, cloudflare:* left external.

import { checkUpgrade, EgressGateway, VTunnelObject } from "../../../ts/src/cloudflare/index.ts";
// @ts-ignore: imported as text, the dynamic Worker's source
import appSource from "./app.js" with { type: "text" };

interface Env {
  EGRESS: DurableObjectNamespace<EgressTunnel>;
  LOADER: WorkerLoader;
  VTUNNEL_UPGRADE_KEY: string;
}

export class EgressTunnel extends VTunnelObject<Env> {
  // The stand's controllers ping every second, so a few seconds of silence
  // is a dead one; the default is for a 30-second heartbeat.
  protected override readonly staleMs = 4_000;

  // Made once per construction: a new value means the object was evicted and
  // built again — woken from hibernation, or restarted.
  private readonly incarnation = crypto.randomUUID();

  async whoami(): Promise<string> {
    return this.incarnation;
  }
}

export class Egress extends EgressGateway<Env, { app: string }> {
  protected tunnel() {
    return this.env.EGRESS.getByName("egress");
  }
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext & { exports: any }): Promise<Response> {
    const url = new URL(request.url);
    const tunnel = env.EGRESS.getByName("egress");
    switch (url.pathname) {
      case "/tunnel": {
        // Only a signed upgrade goes any further than this Worker.
        const refusal = await checkUpgrade(request, env.VTUNNEL_UPGRADE_KEY);
        if (refusal) return new Response(`vtunnel: upgrade refused: ${refusal}\n`, { status: 401 });
        return tunnel.fetch(request);
      }
      case "/status":
        return Response.json({
          incarnation: await tunnel.whoami(),
          controllers: await tunnel.controllers(),
          streams: await tunnel.openStreams(),
          routes: await tunnel.routes(url.searchParams.get("host") ?? "fwd.test:80"),
        });
      case "/run": {
        const app = url.searchParams.get("app") ?? "a";
        const worker = env.LOADER.get(`app-${app}`, () => ({
          compatibilityDate: "2026-10-01",
          mainModule: "app.js",
          modules: { "app.js": appSource as string },
          globalOutbound: ctx.exports.Egress({ props: { app } }),
        }));
        return worker.getEntrypoint().fetch(request);
      }
      default:
        return new Response("not found\n", { status: 404 });
    }
  },
};
