// Runs a yamux session over a standard WebSocket — the one a Durable Object
// gets from WebSocketPair, or a client gets from new WebSocket().
//
// Only binary messages carry the session, and their boundaries mean nothing:
// the Go end (internal/transport/ws) writes whatever its buffers hold as one
// message each. Text messages are the heartbeat ("vtunnel:ping", answered with
// "vtunnel:pong"); any other text cannot come from a vtunnel peer, so it ends
// the session rather than being guessed at.

import { Session, YamuxError, type SessionOptions } from "./yamux.ts";

export const PING_TEXT = "vtunnel:ping";
export const PONG_TEXT = "vtunnel:pong";

export function sessionOverWebSocket(ws: WebSocket, opts: SessionOptions = {}): Session {
  ws.binaryType = "arraybuffer";
  const session = new Session(
    {
      send: (data) => ws.send(data),
      close: () => {
        try {
          ws.close(1000);
        } catch {
          // already closing
        }
      },
    },
    opts,
  );
  ws.addEventListener("message", (event) => {
    const data: unknown = event.data;
    if (data === PING_TEXT) ws.send(PONG_TEXT);
    else if (data === PONG_TEXT) return;
    else if (data instanceof ArrayBuffer) session.receive(new Uint8Array(data));
    else if (ArrayBuffer.isView(data)) session.receive(new Uint8Array(data.buffer, data.byteOffset, data.byteLength));
    else session.close(new YamuxError("protocol_error", "non-binary WebSocket message"));
  });
  ws.addEventListener("close", () => session.transportClosed());
  ws.addEventListener("error", () => session.transportClosed(new YamuxError("session_shutdown", "WebSocket error")));
  return session;
}
