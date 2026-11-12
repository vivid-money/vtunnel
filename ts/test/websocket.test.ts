import { expect, test } from "bun:test";
import { closeWebSocket, PING_TEXT, PONG_TEXT, sessionOverWebSocket } from "../src/websocket.ts";
import { Session, YamuxError } from "../src/yamux.ts";

// The heartbeat is answered beside the session, never fed into it; any other
// text ends the session.
test("heartbeat answered, other text refused", async () => {
  const got: string[] = [];
  let peer: { send(m: string): void } | undefined;
  const opened = Promise.withResolvers<void>();
  const server = Bun.serve({
    port: 0,
    hostname: "127.0.0.1",
    fetch(req, srv) {
      if (srv.upgrade(req)) return;
      return new Response(null, { status: 426 });
    },
    websocket: {
      open(ws) {
        peer = ws;
        opened.resolve();
      },
      message(_, m) {
        got.push(String(m));
      },
    },
  });
  try {
    const ws = new WebSocket(`ws://127.0.0.1:${server.port}/`);
    await new Promise((r) => ws.addEventListener("open", r));
    const session = sessionOverWebSocket(ws);
    await opened.promise;

    peer!.send(PING_TEXT);
    peer!.send(PONG_TEXT);
    await Bun.sleep(50);
    expect(got).toEqual([PONG_TEXT]);
    expect(session.isClosed).toBe(false);

    peer!.send("hello");
    const reason = await session.closed;
    expect((reason as { code?: string }).code).toBe("protocol_error");
  } finally {
    server.stop(true);
  }
});

// A session closed for a reason says so in the close frame, which is what the
// Go end logs; one that just ends, or whose transport ended first, does not.
test("the reason a session is closed reaches the transport", () => {
  const told: (Error | undefined)[] = [];
  const make = () => new Session({ send: () => {}, close: (reason) => told.push(reason) });

  const why = new YamuxError("timeout", "controller stopped answering");
  make().close(why);
  make().close();
  make().transportClosed();
  expect(told).toEqual([why, undefined, undefined]);
});

test("closeWebSocket: 1001 with the reason, 1000 without, and 1000 where 1001 is refused", () => {
  const calls: [number, string | undefined][] = [];
  const ws = { close: (code: number, reason?: string) => void calls.push([code, reason]) } as unknown as WebSocket;
  closeWebSocket(ws, new YamuxError("timeout", "controller stopped answering"));
  closeWebSocket(ws);
  closeWebSocket(ws, new Error("é".repeat(200)));
  expect(calls[0]).toEqual([1001, "vtunnel: controller stopped answering"]);
  expect(calls[1]).toEqual([1000, undefined]);
  expect(new TextEncoder().encode(calls[2]![1]!).length).toBeLessThanOrEqual(123);

  // A client WebSocket takes only 1000 or an application code.
  const strict: number[] = [];
  const client = {
    close: (code: number) => {
      if (code !== 1000) throw new DOMException("InvalidAccessError");
      strict.push(code);
    },
  } as unknown as WebSocket;
  closeWebSocket(client, new Error("gone"));
  expect(strict).toEqual([1000]);
});
