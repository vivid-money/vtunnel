import { expect, test } from "bun:test";
import { PING_TEXT, PONG_TEXT, sessionOverWebSocket } from "../src/websocket.ts";

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
