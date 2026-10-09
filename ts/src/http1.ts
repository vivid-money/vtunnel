// HTTP/1.1 over a byte stream, as a proxy client speaks it — just enough to
// carry a fetch() through a tunnel to the controller's proxy and bring the
// answer back as a Response, without ever holding a whole body.
//
// Requests go out in absolute form ("GET https://host/path HTTP/1.1"), which
// the controller's proxy reads as the client saying what to speak to the
// origin: an https URL is re-issued over TLS there, so nothing on this side
// ever does TLS. One request per
// stream — yamux already multiplexes, and a stream costs nothing — but not
// Connection: close. Asked to close, Go's net/http delimits a body of unknown
// length by closing the connection, and then a body cut short is
// indistinguishable from a whole one. Left to keep the connection, it frames
// that body chunked, whose end is explicit; the stream is closed from this
// side once the response is over.

import { ByteQueue, coalesce } from "./bytes.ts";
import type { ByteReader } from "./frame.ts";

/** Bounds a response's status line and headers together. */
export const MAX_HEAD = 64 * 1024;

export class HttpError extends Error {
  override name = "HttpError";
}

export interface ResponseHead {
  status: number;
  statusText: string;
  /** In arrival order, names as sent. */
  headers: [string, string][];
}

// Hop-by-hop headers (RFC 9110 §7.6.1) and the ones whose job this file does
// itself. None of them describe the request end to end, and passing one on
// would let the application frame the stream behind our back.
const REQUEST_SKIP = new Set([
  "connection",
  "keep-alive",
  "proxy-connection",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
  "host",
  "content-length",
]);

const RESPONSE_SKIP = new Set([
  "connection",
  "keep-alive",
  "proxy-connection",
  "proxy-authenticate",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

const enc = new TextEncoder();
const CRLF = Uint8Array.of(0x0d, 0x0a);
const dec = new TextDecoder();

/** The head of request, ready to write. bodyLength says how its body is framed: a length, or null for chunked, or undefined for no body. */
export function encodeRequestHead(request: Request, bodyLength: number | null | undefined): Uint8Array {
  const url = new URL(request.url);
  url.hash = "";
  let head = `${request.method} ${url.href} HTTP/1.1\r\nHost: ${url.host}\r\n`;
  for (const [name, value] of request.headers) {
    if (!REQUEST_SKIP.has(name)) head += `${name}: ${value}\r\n`;
  }
  if (bodyLength === null) head += "Transfer-Encoding: chunked\r\n";
  else if (bodyLength !== undefined) head += `Content-Length: ${bodyLength}\r\n`;
  head += "\r\n";
  return enc.encode(head);
}

/**
 * How request's body goes on the wire: its declared length if it has a sane
 * one, chunked otherwise, nothing if it has no body. A body-carrying method
 * with no body still says Content-Length: 0, as browsers do, so a server
 * waiting for one is not left guessing.
 */
export function requestBodyLength(request: Request): number | null | undefined {
  if (request.body === null) {
    return ["POST", "PUT", "PATCH"].includes(request.method) ? 0 : undefined;
  }
  const declared = request.headers.get("content-length");
  if (declared !== null && /^[0-9]+$/.test(declared)) return Number(declared);
  return null;
}

/**
 * Writes body to w, framed as requestBodyLength said. A body that turns out
 * longer or shorter than it declared is an error rather than a corrupted
 * stream.
 */
export async function writeRequestBody(
  w: WritableStreamDefaultWriter<Uint8Array>,
  body: ReadableStream<Uint8Array>,
  length: number | null,
): Promise<void> {
  let sent = 0;
  for await (const chunk of coalesce(body)) {
    if (chunk.length === 0) continue;
    sent += chunk.length;
    if (length === null) {
      // One write per chunk, not three: each write is a frame on the tunnel
      // and a message on the WebSocket under it, and the size line and CRLF
      // on their own would triple both.
      const size = enc.encode(`${chunk.length.toString(16)}\r\n`);
      const framed = new Uint8Array(size.length + chunk.length + 2);
      framed.set(size, 0);
      framed.set(chunk, size.length);
      framed.set(CRLF, size.length + chunk.length);
      await w.write(framed);
    } else {
      if (sent > length) throw new HttpError(`request body longer than its Content-Length ${length}`);
      await w.write(chunk);
    }
  }
  if (length === null) await w.write(enc.encode("0\r\n\r\n"));
  else if (sent !== length) throw new HttpError(`request body is ${sent} bytes, Content-Length said ${length}`);
}

/** Response headers worth handing on: everything but hop-by-hop. */
export function responseHeaders(head: ResponseHead): Headers {
  const h = new Headers();
  for (const [name, value] of head.headers) {
    if (!RESPONSE_SKIP.has(name.toLowerCase())) h.append(name, value);
  }
  return h;
}

/** Statuses a Response must be built with a null body for. */
export function isNullBodyStatus(status: number): boolean {
  return status === 101 || status === 103 || status === 204 || status === 205 || status === 304;
}

/**
 * HttpReader reads responses off a stream that delivers bytes in arbitrary
 * pieces: a head, then a body framed the way the head says.
 */
export class HttpReader {
  private readonly buf = new ByteQueue();
  private eof = false;

  constructor(private readonly reader: ByteReader) {}

  /** Reads one response head, skipping interim 1xx responses other than 101. */
  async readHead(): Promise<ResponseHead> {
    for (;;) {
      const head = await this.readOneHead();
      if (head.status >= 100 && head.status < 200 && head.status !== 101) continue;
      return head;
    }
  }

  /**
   * The body of the response whose head was just read, for a request made
   * with method. onEnd runs once it has been read to its end, or failed.
   */
  body(head: ResponseHead, method: string, onEnd: (err?: unknown) => void): ReadableStream<Uint8Array> | null {
    const framing = bodyFraming(head, method);
    if (framing.kind === "none") {
      onEnd();
      return null;
    }
    let done = false;
    const finish = (err?: unknown) => {
      if (done) return;
      done = true;
      onEnd(err);
    };
    let remaining = framing.kind === "length" ? framing.length : 0;
    let chunkLeft = 0; // bytes of the current chunk still to come
    let chunkedDone = false;

    const pull = async (c: ReadableStreamDefaultController<Uint8Array>): Promise<void> => {
      try {
        switch (framing.kind) {
          case "length": {
            if (remaining === 0) {
              c.close();
              return finish();
            }
            await this.fillSome("response body ended before its Content-Length");
            const part = this.buf.take(remaining);
            remaining -= part.length;
            c.enqueue(part);
            if (remaining === 0) {
              c.close();
              finish();
            }
            return;
          }
          case "chunked": {
            while (chunkLeft === 0 && !chunkedDone) {
              const line = await this.readLine("chunk size");
              const size = parseInt(line.split(";")[0]!.trim(), 16);
              if (!/^[0-9a-fA-F]+$/.test(line.split(";")[0]!.trim()) || !Number.isSafeInteger(size)) {
                throw new HttpError(`bad chunk size ${JSON.stringify(line)}`);
              }
              if (size === 0) {
                // Trailers: read and dropped. A Response has nowhere to put them.
                while ((await this.readLine("trailer")) !== "");
                chunkedDone = true;
              } else {
                chunkLeft = size;
              }
            }
            if (chunkedDone) {
              c.close();
              return finish();
            }
            await this.fillSome("response body ended inside a chunk");
            const part = this.buf.take(chunkLeft);
            chunkLeft -= part.length;
            if (chunkLeft === 0) {
              if ((await this.readLine("chunk end")) !== "") throw new HttpError("chunk not followed by CRLF");
            }
            c.enqueue(part);
            return;
          }
          case "close": {
            if (this.buf.length === 0 && !this.eof) await this.fill();
            if (this.buf.length === 0) {
              c.close();
              return finish();
            }
            c.enqueue(this.buf.take(this.buf.length));
            return;
          }
        }
      } catch (e) {
        finish(e);
        throw e;
      }
    };
    return new ReadableStream<Uint8Array>(
      {
        pull,
        cancel: (reason) => {
          finish(reason ?? new HttpError("response body cancelled"));
        },
      },
      { highWaterMark: 0 },
    );
  }

  /** Everything after the head just read, as a stream: for CONNECT. */
  rest(): ReadableStream<Uint8Array> {
    const leftover = this.buf.drain();
    const reader = this.reader;
    const eof = this.eof;
    return new ReadableStream<Uint8Array>(
      {
        async pull(c) {
          const next = leftover.shift();
          if (next) return c.enqueue(next);
          if (eof) return c.close();
          const { value, done } = await reader.read();
          if (done) c.close();
          else c.enqueue(value!);
        },
        cancel: (reason) => reader.cancel(reason),
      },
      { highWaterMark: 0 },
    );
  }

  private async readOneHead(): Promise<ResponseHead> {
    const budget = { left: MAX_HEAD };
    const statusLine = await this.readLine("status line", budget);
    const m = /^HTTP\/1\.[01] ([0-9]{3})(?: (.*))?$/.exec(statusLine);
    if (!m) throw new HttpError(`bad status line ${JSON.stringify(statusLine.slice(0, 100))}`);
    const headers: [string, string][] = [];
    for (;;) {
      const line = await this.readLine("header", budget);
      if (line === "") break;
      if (line.startsWith(" ") || line.startsWith("\t")) throw new HttpError("obsolete header folding");
      const colon = line.indexOf(":");
      if (colon <= 0) throw new HttpError(`bad header line ${JSON.stringify(line.slice(0, 100))}`);
      const name = line.slice(0, colon);
      if (!/^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/.test(name)) throw new HttpError(`bad header name ${JSON.stringify(name)}`);
      headers.push([name, line.slice(colon + 1).trim()]);
    }
    return { status: Number(m[1]), statusText: m[2] ?? "", headers };
  }

  // One line, CRLF (or a bare LF) removed. budget bounds a head as a whole.
  private async readLine(what: string, budget = { left: MAX_HEAD }): Promise<string> {
    for (;;) {
      const at = this.buf.indexOf(0x0a);
      if (at >= 0) {
        budget.left -= at + 1;
        if (budget.left < 0) throw new HttpError(`response head larger than ${MAX_HEAD} bytes`);
        let line = dec.decode(this.buf.read(at + 1));
        line = line.endsWith("\r\n") ? line.slice(0, -2) : line.slice(0, -1);
        return line;
      }
      if (this.buf.length > budget.left) throw new HttpError(`response head larger than ${MAX_HEAD} bytes`);
      if (this.eof) throw new HttpError(`stream ended reading ${what}`);
      await this.fill();
    }
  }

  private async fillSome(eofMessage: string): Promise<void> {
    while (this.buf.length === 0) {
      if (this.eof) throw new HttpError(eofMessage);
      await this.fill();
    }
  }

  private async fill(): Promise<void> {
    const { value, done } = await this.reader.read();
    if (done) this.eof = true;
    else this.buf.push(value!);
  }
}

type Framing = { kind: "none" } | { kind: "length"; length: number } | { kind: "chunked" } | { kind: "close" };

// RFC 9112 §6.3, in its order.
function bodyFraming(head: ResponseHead, method: string): Framing {
  const { status } = head;
  if (method === "HEAD" || status === 204 || status === 304 || (status >= 100 && status < 200)) {
    return { kind: "none" };
  }
  const te = head.headers.filter(([n]) => n.toLowerCase() === "transfer-encoding").map(([, v]) => v);
  if (te.length > 0) {
    const codings = te.join(",").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean);
    return codings.at(-1) === "chunked" ? { kind: "chunked" } : { kind: "close" };
  }
  const cl = head.headers.filter(([n]) => n.toLowerCase() === "content-length").map(([, v]) => v);
  if (cl.length > 0) {
    const values = new Set(cl.join(",").split(",").map((s) => s.trim()));
    const [only] = values;
    if (values.size !== 1 || only === undefined || !/^[0-9]+$/.test(only) || !Number.isSafeInteger(Number(only))) {
      throw new HttpError(`bad Content-Length ${JSON.stringify(cl)}`);
    }
    return { kind: "length", length: Number(only) };
  }
  return { kind: "close" };
}
