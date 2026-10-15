// The signed WebSocket upgrade of protocol yamux-wss, from the accepting side.
// The Go client signs it; both test suites check one golden vector computed
// outside either implementation.
//
//	Authorization: VTunnel-HMAC v1 ts=<unix seconds>,nonce=<16 bytes, base64url>,sig=<base64url>
//	sig = HMAC-SHA256(upgrade key, "vtunnel-upgrade-v1\n" ts "\n" nonce "\n" host "\n" path)
//
// host is lowercased; path is the request target, query included.

const LABEL = "vtunnel-upgrade-v1";
const PREFIX = "VTunnel-HMAC v1 ";
const WINDOW_S = 60;
const MIN_KEY = 32;

export type UpgradeRefusal = "missing" | "malformed" | "expired" | "bad_sig" | "replay";

/** Reads a key written as base64, standard or URL alphabet, padded or not. */
export function parseUpgradeKey(s: string): Uint8Array<ArrayBuffer> {
  const b64 = s.trim().replace(/-/g, "+").replace(/_/g, "/");
  let bytes: Uint8Array<ArrayBuffer>;
  try {
    bytes = Uint8Array.from(atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4)), (c) => c.charCodeAt(0));
  } catch {
    throw new Error("vtunnel: upgrade key is not base64 (openssl rand -base64 32)");
  }
  if (bytes.length < MIN_KEY) {
    throw new Error(`vtunnel: upgrade key is ${bytes.length} bytes, want at least ${MIN_KEY} (openssl rand -base64 32)`);
  }
  return bytes;
}

function message(ts: string, nonce: string, host: string, path: string): Uint8Array<ArrayBuffer> {
  // Copied into a plain ArrayBuffer, which is what WebCrypto's types insist on.
  return new Uint8Array(new TextEncoder().encode(`${LABEL}\n${ts}\n${nonce}\n${host.toLowerCase()}\n${path}`));
}

function b64url(bytes: Uint8Array): string {
  return btoa(String.fromCharCode(...bytes)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

function fromB64url(s: string): Uint8Array<ArrayBuffer> | null {
  if (!/^[A-Za-z0-9_-]*$/.test(s)) return null;
  try {
    const b64 = s.replace(/-/g, "+").replace(/_/g, "/");
    return Uint8Array.from(atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4)), (c) => c.charCodeAt(0));
  } catch {
    return null;
  }
}

async function hmacKey(key: Uint8Array<ArrayBuffer>, usage: "sign" | "verify"): Promise<CryptoKey> {
  return crypto.subtle.importKey("raw", key, { name: "HMAC", hash: "SHA-256" }, false, [usage]);
}

/** The Authorization value a client sends: for tests, and for a TypeScript client. */
export async function signUpgrade(
  key: Uint8Array<ArrayBuffer>,
  host: string,
  path: string,
  nowS = Math.floor(Date.now() / 1000),
  nonce = b64url(crypto.getRandomValues(new Uint8Array(16))),
): Promise<string> {
  const ts = String(nowS);
  const sig = new Uint8Array(await crypto.subtle.sign("HMAC", await hmacKey(key, "sign"), message(ts, nonce, host, path)));
  return `${PREFIX}ts=${ts},nonce=${nonce},sig=${b64url(sig)}`;
}

/**
 * Checks signed upgrades and remembers the nonces it accepted for as long as
 * they could be replayed. One per accepting process: in a Durable Object, one
 * per object, which sees every upgrade for its tunnel.
 */
export class UpgradeVerifier {
  private readonly key: Promise<CryptoKey>;
  private readonly seen = new Map<string, number>(); // nonce -> unix second after which its ts alone refuses it

  constructor(
    key: string | Uint8Array<ArrayBuffer>,
    private readonly now: () => number = () => Math.floor(Date.now() / 1000),
  ) {
    this.key = hmacKey(typeof key === "string" ? parseUpgradeKey(key) : key, "verify");
  }

  /** null if request carries a good signature for where it was sent; otherwise why not. */
  async verify(request: Request): Promise<UpgradeRefusal | null> {
    const url = new URL(request.url);
    return this.check(request.headers.get("authorization"), url.host, url.pathname + url.search);
  }

  async check(auth: string | null, host: string, path: string): Promise<UpgradeRefusal | null> {
    if (!auth) return "missing";
    if (!auth.startsWith(PREFIX)) return "malformed";
    const fields = new Map<string, string>();
    for (const part of auth.slice(PREFIX.length).split(",")) {
      const eq = part.indexOf("=");
      if (eq < 0) return "malformed";
      fields.set(part.slice(0, eq).trim(), part.slice(eq + 1));
    }
    const ts = fields.get("ts") ?? "";
    const nonce = fields.get("nonce") ?? "";
    const sig = fromB64url(fields.get("sig") ?? "");
    if (!/^-?[0-9]+$/.test(ts) || nonce.length < 16 || !sig) return "malformed";

    const now = this.now();
    const when = Number(ts);
    if (Math.abs(when - now) > WINDOW_S) return "expired";
    const ok = await crypto.subtle.verify("HMAC", await this.key, sig, message(ts, nonce, host, path));
    if (!ok) return "bad_sig";

    for (const [n, until] of this.seen) if (now > until) this.seen.delete(n);
    if (this.seen.has(nonce)) return "replay";
    this.seen.set(nonce, when + WINDOW_S);
    return null;
  }
}
