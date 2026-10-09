// A Worker that lets code running in Cloudflare reach a private network
// through vtunnel controllers that dial in from it.
//
//   /tunnel       controllers connect here (protocol yamux-wss, signed upgrade)
//   /demo/fetch   this Worker fetching through the tunnel (?url=, default
//                 http://api.internal/hello)
//   /demo/app     a Dynamic Worker whose whole network is the tunnel

import { checkUpgrade, EgressGateway, VTunnelObject } from "@vivid-money/vtunnel/cloudflare";

interface Env {
  EGRESS: DurableObjectNamespace<EgressTunnel>;
  LOADER: WorkerLoader;
  VTUNNEL_UPGRADE_KEY: string;
}

/** The tunnel. One instance holds every controller. */
export class EgressTunnel extends VTunnelObject<Env> {}

/**
 * The gateway: anything whose network should be the tunnel calls this. A
 * Dynamic Worker gets it as its globalOutbound; this Worker can call it too.
 */
export class Egress extends EgressGateway<Env> {
  protected tunnel() {
    return this.env.EGRESS.getByName("egress");
  }
}

// The code a Dynamic Worker runs in the demo. Its fetch() cannot reach the
// internet: globalOutbound sends it to the gateway, and the gateway to the
// tunnel.
const demoApp = `
export default {
  async fetch() {
    const r = await fetch("http://api.internal/hello");
    return new Response(await r.text(), { status: r.status, headers: { "content-type": "application/json" } });
  },
};
`;

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext & { exports: any }): Promise<Response> {
    const url = new URL(request.url);
    switch (url.pathname) {
      case "/tunnel": {
        // Only a signed upgrade gets past this Worker; the object checks it
        // again and refuses a replay.
        const refusal = await checkUpgrade(request, env.VTUNNEL_UPGRADE_KEY);
        if (refusal) return new Response(`vtunnel: upgrade refused: ${refusal}\n`, { status: 401 });
        return env.EGRESS.getByName("egress").fetch(request);
      }

      case "/demo/fetch":
        // Any URL, but only hosts a controller routes are reached; the rest
        // get a 502 from the tunnel object, and nothing leaves Cloudflare.
        return ctx.exports.Egress({}).fetch(url.searchParams.get("url") ?? "http://api.internal/hello");

      case "/demo/app": {
        const worker = env.LOADER.get("demo-app", () => ({
          compatibilityDate: "2026-10-01",
          mainModule: "app.js",
          modules: { "app.js": demoApp },
          globalOutbound: ctx.exports.Egress({}),
        }));
        return worker.getEntrypoint().fetch(request);
      }

      default:
        return new Response("not found\n", { status: 404 });
    }
  },
};
