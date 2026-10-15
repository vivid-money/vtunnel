// What code in this isolate reaches the controller's network through: fetch()
// and connect(), each carried on its own tunnel stream to the controller's
// proxy. The requests arrive already parsed, from the runtime, so all that is
// left to do is chain them into the tunnel.
//
// Both speak to the controller's proxy as any proxy client would:
//
//   - fetch: one request in absolute form. An https URL is re-issued over TLS
//     by the controller, so the request crosses the tunnel in the clear and
//     no certificate authority is involved on this side at all.
//   - connect: CONNECT host:port, then bytes both ways. The controller's
//     proxy decides what those bytes are.
//
// A host no controller routes is refused here, before anything is sent: there
// is no fallback to the internet.

import { coalesce } from "./bytes.ts";
import { HttpReader, encodeRequestHead, isNullBodyStatus, requestBodyLength, responseHeaders, writeRequestBody } from "./http1.ts";
import { hostPortOf } from "./routes.ts";
import { NoRouteError, type TunnelServer } from "./server.ts";

export interface TunnelEgressOptions {
  /** How long to wait for a response head once the request is sent. Default 60 s. 0 disables. */
  responseHeaderTimeoutMs?: number;
}

/** connect() was refused by the controller's proxy, or there was no route. */
export class ConnectError extends Error {
  override name = "ConnectError";
}

/** The two halves of a socket, as connect() in Workers hands them out. */
export interface Duplex {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
}

export class TunnelEgress {
  private readonly responseHeaderTimeoutMs: number;

  constructor(
    private readonly server: TunnelServer,
    opts: TunnelEgressOptions = {},
  ) {
    this.responseHeaderTimeoutMs = opts.responseHeaderTimeoutMs ?? 60_000;
  }

  /**
   * Sends request through the tunnel and returns the response as it starts to
   * arrive; the body streams. A host with no route gets a 502 made here, saying
   * so, rather than a network error the application cannot tell from an outage.
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.protocol !== "http:" && url.protocol !== "https:") {
      throw new TypeError(`vtunnel: cannot fetch ${url.protocol} URLs`);
    }
    if (request.headers.get("upgrade") !== null) {
      // Needs a 101 and a WebSocket handed back, which is its own path.
      return refusal(501, "vtunnel: protocol upgrades are not supported yet");
    }
    const hostPort = hostPortOf(url);

    let tunnel;
    try {
      tunnel = await this.server.openTunnel(hostPort);
    } catch (e) {
      if (e instanceof NoRouteError) return refusal(502, `vtunnel: no route for ${hostPort}`);
      throw e;
    }
    const { stream } = tunnel;
    const writer = stream.writable.getWriter();
    const http = new HttpReader(stream.readable.getReader());
    const abort = () => stream.reset();
    request.signal?.addEventListener("abort", abort);

    // The write side is closed only once the response is over. Go's net/http,
    // on the far side, cancels a request whose client half-closes, so FIN after
    // the request — the natural thing for a byte pipe — would turn every slow
    // answer into a 502.
    let ended = false;
    const end = (err?: unknown) => {
      if (ended) return;
      ended = true;
      request.signal?.removeEventListener("abort", abort);
      if (err !== undefined) stream.reset();
      else void writer.close().catch(() => stream.reset());
    };

    try {
      const length = requestBodyLength(request);
      await writer.write(encodeRequestHead(request, length));
      // The body goes out alongside reading the answer, not before it: a
      // server may answer early (a 413, a redirect) and stop reading.
      const sending =
        request.body && length !== undefined ? writeRequestBody(writer, request.body, length) : Promise.resolve();
      sending.catch((e) => end(e));

      const head = await withTimeout(http.readHead(), this.responseHeaderTimeoutMs);
      if (head.status === 101) throw new TypeError("vtunnel: 101 Switching Protocols without an upgrade request");
      const body = http.body(head, request.method, end);
      return new Response(isNullBodyStatus(head.status) ? null : body, {
        status: head.status,
        statusText: head.statusText,
        headers: responseHeaders(head),
      });
    } catch (e) {
      end(e);
      throw e;
    }
  }

  /**
   * Connects socket to hostPort through the tunnel and pipes it both ways
   * until both directions are done. Half-close passes through. Rejects with
   * ConnectError, before touching socket, if there is no route or the
   * controller's proxy refuses.
   */
  async connect(hostPort: string, socket: Duplex): Promise<void> {
    let tunnel;
    try {
      tunnel = await this.server.openTunnel(hostPort);
    } catch (e) {
      if (e instanceof NoRouteError) throw new ConnectError(e.message);
      throw e;
    }
    const { stream } = tunnel;
    const writer = stream.writable.getWriter();
    const http = new HttpReader(stream.readable.getReader());
    try {
      await writer.write(new TextEncoder().encode(`CONNECT ${hostPort} HTTP/1.1\r\nHost: ${hostPort}\r\n\r\n`));
      const head = await withTimeout(http.readHead(), this.responseHeaderTimeoutMs);
      if (head.status !== 200) {
        throw new ConnectError(`vtunnel: CONNECT ${hostPort}: ${head.status} ${head.statusText}`.trim());
      }
    } catch (e) {
      stream.reset();
      throw e;
    }
    writer.releaseLock();

    const up = (async () => {
      const w = stream.writable.getWriter();
      for await (const chunk of coalesce(socket.readable)) await w.write(chunk);
      await w.close();
    })().catch((e) => {
      void socket.readable.cancel(e).catch(() => {});
      throw e;
    });
    const down = http.rest().pipeTo(socket.writable);
    const [u, d] = await Promise.allSettled([up, down]);
    if (u.status === "rejected" || d.status === "rejected") stream.reset();
  }
}

function refusal(status: number, message: string): Response {
  return new Response(message + "\n", {
    status,
    headers: { "content-type": "text/plain; charset=utf-8", "x-vtunnel-error": status === 502 ? "no-route" : "unsupported" },
  });
}

async function withTimeout<T>(work: Promise<T>, ms: number): Promise<T> {
  if (ms <= 0) return work;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`vtunnel: no response within ${ms} ms`)), ms);
  });
  try {
    return await Promise.race([work, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}
