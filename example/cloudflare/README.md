# vtunnel on Cloudflare

Code running in Cloudflare — this Worker, a Dynamic Worker, a Sandbox SDK container — reaching services in a private network, with nothing opened into that network. A controller inside it dials **out** to a Durable Object over `wss://`, and Cloudflare-side traffic comes back down that connection to the controller's proxy, which injects credentials and makes the real request.

```
 CLOUDFLARE                                      PRIVATE NETWORK

 Dynamic Worker ── fetch() ─┐
                            ▼
 this Worker ──────────▶ Egress (gateway)        vtunnel client
                            │                      + MITM proxy, credentials
                            ▼                      │
                     EgressTunnel (DO) ◀═══════════╡ yamux-wss, dialled out
                                                   │
                                                   └──▶ api.internal
```

| File | What it is |
|---|---|
| `src/index.ts` | The Worker: `EgressTunnel` (the Durable Object), `Egress` (the gateway), and the routes |
| `wrangler.jsonc` | Bindings: the Durable Object, the Worker Loader, the upgrade key secret |
| `test.sh` | Runs all of it locally under `wrangler dev` and checks it |

Routes:

| Route | |
|---|---|
| `/tunnel` | where controllers connect; anything but a signed upgrade gets 401 |
| `/demo/fetch` | this Worker fetching `http://api.internal/hello` through the tunnel; `?url=` for another — a host no controller routes gets a 502 |
| `/demo/app` | a Dynamic Worker doing the same — its only network is the tunnel |

## Try it locally

```bash
./example/cloudflare/test.sh
```

It starts `wrangler dev` over HTTPS with a throwaway certificate, a stand-in internal service, and a controller, then checks that both demo routes reach the service with the credential the controller injected, that the service really received those requests, that a host no controller routes gets a 502 without leaving, and that an unsigned upgrade is refused. Needs Node, Go, Python 3 and openssl.

## Deploy

**1. The upgrade key.** A random value both sides hold; controllers sign their connection with it. It is not the tunnel secret of sandbox tunnels and has nothing to do with it.

```bash
openssl rand -base64 32
```

**2. The Worker.**

```bash
cd example/cloudflare
npm install
npx wrangler secret put VTUNNEL_UPGRADE_KEY   # paste the key
npx wrangler deploy
```

**3. The controller**, anywhere in the private network that can reach `*.workers.dev` (or your route) over HTTPS:

```bash
vtunnel client \
  -protocol yamux-wss \
  -server wss://vtunnel-egress.<account>.workers.dev/tunnel \
  -upgrade-key "$VTUNNEL_UPGRADE_KEY" \
  -mitm-ca ./mitm-ca.pem \
  -forward api.internal=https://api.internal.corp \
  -H "Authorization: Bearer $API_TOKEN"
```

Or from Go, with routes declared on the client's proxy:

```go
client := vtunnel.NewClient("wss://vtunnel-egress.<account>.workers.dev/tunnel",
    vtunnel.WithProtocol(vtunnel.ProtocolYamuxWSS),
    vtunnel.WithUpgradeKey(os.Getenv("VTUNNEL_UPGRADE_KEY")),
    vtunnel.WithMitm(ca),
)
client.Proxy().ForwardTo("api.internal", "https://api.internal.corp",
    vtunnel.WithHeader("Authorization", "Bearer "+token))
client.Connect()
```

**4. Check:** `curl https://vtunnel-egress.<account>.workers.dev/demo/fetch`.

## Using it from your own Worker

The library is `ts/` (`@vivid-money/vtunnel`). Export the two classes, bind the object, and point whatever should use the tunnel at the gateway:

```ts
import { checkUpgrade, EgressGateway, VTunnelObject } from "@vivid-money/vtunnel/cloudflare";

export class EgressTunnel extends VTunnelObject<Env> {}
export class Egress extends EgressGateway<Env> {
  protected tunnel() { return this.env.EGRESS.getByName("egress"); }
}

// a Dynamic Worker whose network is the tunnel:
env.LOADER.get(id, () => ({ ..., globalOutbound: ctx.exports.Egress({}) }));
```

The `/tunnel` route must check the signature before passing the upgrade on — `checkUpgrade`, as in `src/index.ts`. The object checks it again and refuses replays.

## Operating it

- **Several controller pods** connect to the same object at once. Each declares its routes; calls go round-robin among the pods that route the host. A pod that dies is dropped — at once if its connection closes, within 90 seconds of silence if it vanishes without closing (the object needs the client's keepalive for that: leave `-keepalive` at its default).
- **Idle costs nothing.** The object hibernates while no call is in flight; controllers stay connected and their keepalive is answered without waking it.
- **A deploy disconnects every controller.** They reconnect on their own; calls in flight at that moment fail as network errors.
- **Hosts without a route** get a 502 made by the object (`x-vtunnel-error: no-route`); nothing is sent to the internet.
- **Rotating the upgrade key** means changing it in both places; controllers on the old key are refused until they have the new one.
- **Whoever terminates the `wss://` — Cloudflare — sees the tunnel in the clear.** Credentials are injected on the controller, after the tunnel, so they never cross it; request and response bodies do.

What does not work, and why:

- **gRPC through `fetch()`**: the Fetch API exposes no response trailers, and gRPC's status is in them.
- **`connect()` with `secureTransport: "on"`**: workerd refuses to hand TLS to the gateway. Plain `connect()` works and is piped to the route's target.
- **A body of unknown length cut short** (chunked, SSE) reads as ended to the application; a body of known length fails instead.
