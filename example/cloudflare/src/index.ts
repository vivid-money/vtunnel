// A Worker that lets code running in Cloudflare reach a private network
// through vtunnel controllers that dial in from it.
//
//   /tunnel       controllers connect here (protocol yamux-wss, signed upgrade)
//   /demo/fetch   this Worker fetching through the gateway (?url=, default
//                 http://api.internal/hello)
//   /demo/app     a Dynamic Worker whose whole network is the tunnel

import { checkUpgrade, EgressGateway, hostPortOf, VTunnelObject } from "@vivid-money/vtunnel/cloudflare";

interface Env {
  EGRESS: DurableObjectNamespace<EgressTunnel>;
  LOADER: WorkerLoader;
  VTUNNEL_UPGRADE_KEY: string;
}

/** The tunnel. One instance holds every controller. */
export class EgressTunnel extends VTunnelObject<Env> {}

/**
 * The gateway: a Dynamic Worker gets it as its globalOutbound, and this
 * Worker can call it too. Hosts a controller routes go through the tunnel;
 * this example sends everything else straight to the internet. Leave fetch()
 * alone and they get a 502 instead, or decide anything else here.
 */
export class Gateway extends EgressGateway<Env> {
  protected tunnel() {
    return this.env.EGRESS.getByName("egress");
  }

  override async fetch(request: Request): Promise<Response> {
    if (await this.tunnel().routes(hostPortOf(request.url))) return super.fetch(request);
    return fetch(request);
  }
}

// The code a Dynamic Worker runs in the demo. Its fetch() goes to the gateway,
// whatever the URL: globalOutbound leaves it no other way out.
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
        return ctx.exports.Gateway({}).fetch(url.searchParams.get("url") ?? "http://api.internal/hello");

      case "/demo/app": {
        const worker = env.LOADER.get("demo-app", () => ({
          compatibilityDate: "2026-10-01",
          mainModule: "app.js",
          modules: { "app.js": demoApp },
          globalOutbound: ctx.exports.Gateway({}),
        }));
        return worker.getEntrypoint().fetch(request);
      }

      default:
        return new Response("not found\n", { status: 404 });
    }
  },
};
