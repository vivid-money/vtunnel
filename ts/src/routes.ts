// Which route a host belongs to. The controlplane matches the same name again
// behind the tunnel, so the two must read a name identically: a mismatch here
// cannot open anything, but it can refuse what the controlplane would have
// served, or send it to a session that will answer 403.
//
// Known gaps, both harmless in practice: unicode is turned into punycode with
// WHATWG URL's IDNA, which maps more than the controlplane's Punycode profile
// does; and an address or network as a key is recognised by shape rather than
// parsed, which only decides whether a trailing ":n" is a port.

const enum Kind {
  Rightmost, // "prefix.*"
  Leftmost, // "*.suffix"
  Exact,
}

interface RouteKey {
  host: string; // canonical, wildcard included
  port: string; // "" matches every port
  kind: Kind;
}

/**
 * Picks the pattern that best matches hostPort, or null. Priority: exact
 * beats wildcard, leftmost beats rightmost, a longer host beats a shorter one,
 * then a pattern naming a port beats one that does not; a tie goes to the
 * smaller spelling, so the answer never moves.
 */
export function bestDomainMatch(patterns: Iterable<string>, hostPort: string): string | null {
  const all = [...patterns];
  if (all.includes(hostPort)) return hostPort;

  const canonical = canonicalHostPort(hostPort);
  if (canonical === null) return null;
  const [host, port] = splitHostPort(canonical)!;

  let bestPattern: string | null = null;
  let best: RouteKey | null = null;
  for (const pattern of all) {
    const key = parseRouteKey(pattern);
    if (!key || !matches(key, host, port)) continue;
    if (best !== null && bestPattern !== null && !beats(key, best)) {
      if (beats(best, key) || pattern >= bestPattern) continue;
    }
    bestPattern = pattern;
    best = key;
  }
  return bestPattern;
}

/** The one spelling of an authority that route tables are keyed by, or null if it names no host. */
export function canonicalHostPort(hostPort: string): string | null {
  const split = splitHostPort(hostPort);
  if (!split) return null;
  const host = canonicalHost(split[0]);
  if (host === "") return null;
  return joinHostPort(host, canonicalPort(split[1]));
}

/** Folds letter case, a trailing dot and unicode (to punycode). "" means no host. */
export function canonicalHost(host: string): string {
  host = host.toLowerCase();
  if (host.endsWith(".")) host = host.slice(0, -1);
  if (host.replaceAll(".", "") === "") return "";
  if (isASCII(host)) return host;
  // Label by label, so that the `*` of a wildcard pattern survives: a URL
  // refuses it as a hostname.
  return host
    .split(".")
    .map((label) => {
      if (isASCII(label)) return label;
      try {
        return new URL(`http://${label}/`).hostname;
      } catch {
        return label;
      }
    })
    .join(".");
}

/** Folds the zero padding a dialer ignores: "0443" is 443. A service name is left alone. */
export function canonicalPort(port: string): string {
  const trimmed = port.replace(/^0+/, "");
  if (trimmed === "") return port;
  return /^[0-9]+$/.test(trimmed) ? trimmed : port;
}

/** net.SplitHostPort: brackets for IPv6, otherwise exactly one colon. */
export function splitHostPort(hostPort: string): [string, string] | null {
  if (hostPort.startsWith("[")) {
    const end = hostPort.indexOf("]");
    if (end < 0 || hostPort[end + 1] !== ":") return null;
    const host = hostPort.slice(1, end);
    const port = hostPort.slice(end + 2);
    if (host.includes("[") || host.includes("]") || port.includes("[") || port.includes("]")) return null;
    return [host, port];
  }
  const i = hostPort.lastIndexOf(":");
  if (i < 0) return null;
  const host = hostPort.slice(0, i);
  if (host.includes(":") || host.includes("[") || host.includes("]")) return null;
  const port = hostPort.slice(i + 1);
  if (port.includes("[") || port.includes("]")) return null;
  return [host, port];
}

/**
 * The host and port a URL reaches, as route tables and routes() take them:
 * "api.corp:443" for https://api.corp/, the port a scheme implies filled in.
 */
export function hostPortOf(url: string | URL): string {
  const u = typeof url === "string" ? new URL(url) : url;
  const host = u.hostname.startsWith("[") && u.hostname.endsWith("]") ? u.hostname.slice(1, -1) : u.hostname;
  return joinHostPort(host, u.port || (u.protocol === "https:" || u.protocol === "wss:" ? "443" : "80"));
}

export function joinHostPort(host: string, port: string): string {
  return host.includes(":") ? `[${host}]:${port}` : `${host}:${port}`;
}

function parseRouteKey(pattern: string): RouteKey | null {
  const split = splitRule(pattern);
  if (!split) return null;
  const host = canonicalHost(split[0]);
  if (host === "") return null;
  return { host, port: split[1], kind: classify(host) };
}

// The port is optional, so failing to split is the ordinary case. Only a bracketed host or a single colon means a port.
function splitRule(raw: string): [string, string] | null {
  raw = raw.trim();
  if (raw === "") return null;
  if (looksLikeAddress(raw)) return [raw, ""];
  const split = splitHostPort(raw);
  if (!split) return [raw, ""];
  const [host, port] = split;
  if (port === "" || !isPortNumber(port)) return null;
  return [host, canonicalPort(port)];
}

// An IP address or network standing on its own, colons and all — what
// a rule may be. Zoned addresses are not.
function looksLikeAddress(raw: string): boolean {
  let s = raw;
  if (s.startsWith("[") && s.endsWith("]")) s = s.slice(1, -1);
  const slash = s.indexOf("/");
  if (slash >= 0) {
    if (!/^[0-9]{1,3}$/.test(s.slice(slash + 1))) return false;
    s = s.slice(0, slash);
  }
  if (/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.test(s)) {
    return s.split(".").every((o) => Number(o) <= 255);
  }
  return s.includes(":") && /^[0-9a-fA-F:.]+$/.test(s) && (s.match(/::/g)?.length ?? 0) <= 1;
}

function isPortNumber(port: string): boolean {
  if (!/^[0-9]+$/.test(port)) return false;
  const n = Number(port);
  return n >= 1 && n <= 65535;
}

function classify(host: string): Kind {
  if (host.startsWith("*.")) return Kind.Leftmost;
  if (host.endsWith(".*")) return Kind.Rightmost;
  return Kind.Exact;
}

function matches(key: RouteKey, host: string, port: string): boolean {
  if (key.port !== "" && key.port !== port) return false;
  if (key.kind === Kind.Exact) return key.host === host;
  if (key.kind === Kind.Leftmost) {
    const suffix = key.host.slice(1); // ".suffix"
    return host.endsWith(suffix) && host.length > suffix.length;
  }
  const prefix = key.host.slice(0, -1); // "prefix."
  return host.startsWith(prefix) && host.length > prefix.length;
}

function beats(k: RouteKey, other: RouteKey): boolean {
  if (k.kind !== other.kind) return k.kind > other.kind;
  if (k.host.length !== other.host.length) return k.host.length > other.host.length;
  return k.port !== "" && other.port === "";
}

function isASCII(s: string): boolean {
  for (let i = 0; i < s.length; i++) if (s.charCodeAt(i) >= 0x80) return false;
  return true;
}
