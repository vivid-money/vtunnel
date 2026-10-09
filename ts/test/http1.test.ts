import { describe, expect, test } from "bun:test";
import { HttpReader, encodeRequestHead, requestBodyLength } from "../src/http1.ts";
import type { ByteReader } from "../src/frame.ts";
import { readAll, str, text } from "./helpers.ts";

// Delivers wire one byte at a time unless told otherwise, then ends.
function source(wire: string | Uint8Array, step = 1): ByteReader {
  const bytes = typeof wire === "string" ? text(wire) : wire;
  let at = 0;
  return new ReadableStream<Uint8Array>({
    pull(c) {
      if (at >= bytes.length) return c.close();
      c.enqueue(bytes.slice(at, at + step));
      at += step;
    },
  }).getReader();
}

async function parse(wire: string, method = "GET", step = 1) {
  const r = new HttpReader(source(wire, step));
  const head = await r.readHead();
  let ended: unknown = "not yet";
  const body = r.body(head, method, (e) => (ended = e));
  return { head, body: body ? str(await readAll(body)) : null, ended: () => ended };
}

describe("response parsing", () => {
  test("content-length, split byte by byte", async () => {
    const { head, body, ended } = await parse("HTTP/1.1 200 OK\r\nContent-Length: 5\r\nX-A: 1\r\n\r\nhelloEXTRA");
    expect(head).toEqual({ status: 200, statusText: "OK", headers: [["Content-Length", "5"], ["X-A", "1"]] });
    expect(body).toBe("hello");
    expect(ended()).toBeUndefined();
  });

  test("chunked, with extensions and trailers", async () => {
    const wire =
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
      "5;ext=1\r\nhello\r\n" +
      "1A\r\nabcdefghijklmnopqrstuvwxyz\r\n" +
      "0\r\nX-Trailer: t\r\n\r\n";
    expect((await parse(wire)).body).toBe("helloabcdefghijklmnopqrstuvwxyz");
    expect((await parse(wire, "GET", 7)).body).toBe("helloabcdefghijklmnopqrstuvwxyz");
  });

  test("transfer-encoding wins over content-length", async () => {
    const wire = "HTTP/1.1 200 OK\r\nContent-Length: 100\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n";
    expect((await parse(wire)).body).toBe("abc");
  });

  test("no framing: read to close", async () => {
    expect((await parse("HTTP/1.0 200 OK\r\n\r\nuntil the end", "GET", 4)).body).toBe("until the end");
  });

  test("interim 1xx skipped; HEAD, 204, 304 have no body", async () => {
    const { head, body } = await parse("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early\r\nLink: x\r\n\r\nHTTP/1.1 201 Created\r\nContent-Length: 2\r\n\r\nok");
    expect([head.status, body]).toEqual([201, "ok"]);
    expect((await parse("HTTP/1.1 200 OK\r\nContent-Length: 99\r\n\r\n", "HEAD")).body).toBeNull();
    expect((await parse("HTTP/1.1 204 No Content\r\n\r\n")).body).toBeNull();
    expect((await parse("HTTP/1.1 304 Not Modified\r\nContent-Length: 10\r\n\r\n")).body).toBeNull();
  });

  test("bare LF line endings are accepted", async () => {
    expect((await parse("HTTP/1.1 200 OK\nContent-Length: 2\n\nhi")).body).toBe("hi");
  });

  test("malformed responses are errors, not guesses", async () => {
    for (const wire of [
      "HTTP/2 200 OK\r\n\r\n",
      "HTTP/1.1 20 OK\r\n\r\n",
      "garbage\r\n\r\n",
      "HTTP/1.1 200 OK\r\nno colon\r\n\r\n",
      "HTTP/1.1 200 OK\r\nX-A: 1\r\n folded\r\n\r\n",
      "HTTP/1.1 200 OK\r\nBad Name: 1\r\n\r\n",
      "HTTP/1.1 200 OK\r\n", // ends inside the head
    ]) {
      await expect(new HttpReader(source(wire)).readHead()).rejects.toThrow();
    }
    for (const wire of [
      "HTTP/1.1 200 OK\r\nContent-Length: 5, 6\r\n\r\nhello",
      "HTTP/1.1 200 OK\r\nContent-Length: -1\r\n\r\n",
      "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nshort",
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabcX\r\n",
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab",
    ]) {
      await expect(parse(wire)).rejects.toThrow();
    }
  });

  test("a head larger than 64 KiB is refused", async () => {
    const wire = "HTTP/1.1 200 OK\r\n" + `X-Big: ${"x".repeat(70_000)}\r\n\r\n`;
    await expect(new HttpReader(source(wire, 4096)).readHead()).rejects.toThrow("larger than");
    const many = "HTTP/1.1 200 OK\r\n" + "X-A: 1234567890\r\n".repeat(5000) + "\r\n";
    await expect(new HttpReader(source(many, 4096)).readHead()).rejects.toThrow("larger than");
  });

  test("identical repeated Content-Length is fine", async () => {
    expect((await parse("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nContent-Length: 2\r\n\r\nok")).body).toBe("ok");
  });
});

describe("request head", () => {
  test("absolute form, Host from the URL, hop-by-hop dropped, no Connection: close", () => {
    const req = new Request("https://API.corp:8443/p?q=1#frag", {
      method: "POST",
      headers: { "Content-Type": "text/plain", "X-A": "1", Connection: "keep-alive", Host: "spoofed", "Transfer-Encoding": "gzip", TE: "trailers" },
      body: "x",
    });
    expect(str(encodeRequestHead(req, null))).toBe(
      "POST https://api.corp:8443/p?q=1 HTTP/1.1\r\nHost: api.corp:8443\r\n" +
        "content-type: text/plain\r\nx-a: 1\r\n" +
        "Transfer-Encoding: chunked\r\n\r\n",
    );
  });

  test("body framing", () => {
    expect(requestBodyLength(new Request("http://a/"))).toBeUndefined();
    expect(requestBodyLength(new Request("http://a/", { method: "POST" }))).toBe(0);
    expect(requestBodyLength(new Request("http://a/", { method: "POST", body: "abc", headers: { "content-length": "3" } }))).toBe(3);
    expect(requestBodyLength(new Request("http://a/", { method: "POST", body: "abc" }))).toBeNull();
  });
});
