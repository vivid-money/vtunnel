// yamux, wire-compatible with github.com/hashicorp/yamux v0.1.2, which the Go
// end of vtunnel links (go.mod).
//
// A Durable Object has one thread, no goroutines and no blocking reads, so
// waiting is a promise and a stream is a pair of Web Streams — the same shape
// as the Socket that connect() hands a Worker, so the two pipe into each other.
//
// Keepalive is deliberately absent, as it is on the Go end: vtunnel pings at
// its own layer, the same way on every backend. ping() exists for tests and for
// anyone who wants an RTT.

import { ByteQueue } from "./bytes.ts";

const PROTO_VERSION = 0;

const TYPE_DATA = 0;
const TYPE_WINDOW_UPDATE = 1;
const TYPE_PING = 2;
const TYPE_GO_AWAY = 3;

const FLAG_SYN = 1 << 0;
const FLAG_ACK = 1 << 1;
const FLAG_FIN = 1 << 2;
const FLAG_RST = 1 << 3;

const GO_AWAY_NORMAL = 0;
const GO_AWAY_PROTO_ERR = 1;
const GO_AWAY_INTERNAL_ERR = 2;

const HEADER_SIZE = 12;

/** Every stream starts with this window; growing it is a WindowUpdate away. */
export const INITIAL_STREAM_WINDOW = 256 * 1024;

export type YamuxErrorCode =
  | "session_shutdown"
  | "remote_go_away"
  | "connection_reset"
  | "stream_closed"
  | "streams_exhausted"
  | "duplicate_stream"
  | "recv_window_exceeded"
  | "unexpected_flag"
  | "invalid_version"
  | "invalid_msg_type"
  | "protocol_error"
  | "timeout";

export class YamuxError extends Error {
  constructor(
    readonly code: YamuxErrorCode,
    message: string = code.replaceAll("_", " "),
  ) {
    super(message);
    this.name = "YamuxError";
  }
}

/**
 * Transport is the byte pipe under a session. send must not reorder, and must
 * either take the bytes or throw: yamux has no way to recover a frame that went
 * missing in the middle of the wire.
 */
export interface Transport {
  send(data: Uint8Array): void;
  close(): void;
}

export interface SessionOptions {
  /** Client sessions open odd stream IDs, servers even. Default: server. */
  client?: boolean;

  /**
   * Per-stream receive window, in bytes. The Go end uses 8 MiB. That figure buys
   * throughput on a fat link at the cost of buffering up to that much per
   * stream whose reader falls behind; a Durable Object has 128 MB in total,
   * so the default here is the smaller 1 MiB. At least INITIAL_STREAM_WINDOW.
   */
  maxStreamWindow?: number;

  /** Streams accepted but not yet taken by accept(), and SYNs this end may have in flight. Default 256, matching the Go end. */
  acceptBacklog?: number;

  /** Largest data frame this end sends. Default 64 KiB. */
  maxFramePayload?: number;

  /** How long an opened stream may wait for its ACK before the session is declared dead. Default 75 s. 0 disables. */
  openTimeoutMs?: number;

  /** How long a half-closed stream waits for the peer's FIN before it is reset. Default 5 min. 0 disables. */
  closeTimeoutMs?: number;
}

// Signal is a channel with no value: wait for the next notify.
class Signal {
  private waiters: (() => void)[] = [];
  wait(): Promise<void> {
    return new Promise((resolve) => this.waiters.push(resolve));
  }
  notify(): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const resolve of waiters) resolve();
  }
}

type Timer = ReturnType<typeof setTimeout>;

export class Session {
  /** Settles when the session ends, with the reason it ended. Never rejects. */
  readonly closed: Promise<Error>;

  /** @internal */ readonly maxStreamWindow: number;
  /** @internal */ readonly maxFramePayload: number;
  /** @internal */ readonly closeTimeoutMs: number;
  private readonly acceptBacklog: number;
  private readonly openTimeoutMs: number;

  private nextStreamId: number;
  private readonly streams = new Map<number, Stream>();
  // inflight holds streams this end opened that the peer has not ACKed. Go
  // bounds them with synCh, sized like the peer's accept backlog, so that an
  // opener blocks instead of being reset for exceeding it.
  private readonly inflight = new Map<number, Timer | undefined>();
  private readonly inflightSignal = new Signal();
  private acceptQueue: Stream[] = [];
  private readonly acceptSignal = new Signal();

  private localGoAway = false;
  private remoteGoAway = false;

  private nextPingId = 0;
  private readonly pings = new Map<number, { resolve: () => void; reject: (e: Error) => void }>();

  private readonly rx = new ByteQueue();
  // A data frame whose header has been read and whose payload is still
  // arriving. stream is null when the payload is being thrown away.
  private pending: { stream: Stream | null; flags: number; remaining: number; parts: Uint8Array[] } | null = null;

  private closeErr: Error | null = null;
  private resolveClosed!: (reason: Error) => void;

  constructor(
    private readonly transport: Transport,
    opts: SessionOptions = {},
  ) {
    this.nextStreamId = opts.client ? 1 : 2;
    this.maxStreamWindow = opts.maxStreamWindow ?? 1024 * 1024;
    if (this.maxStreamWindow < INITIAL_STREAM_WINDOW || this.maxStreamWindow > 0xffffffff) {
      throw new RangeError(`maxStreamWindow ${this.maxStreamWindow} outside [${INITIAL_STREAM_WINDOW}, 2^32)`);
    }
    this.acceptBacklog = opts.acceptBacklog ?? 256;
    this.maxFramePayload = opts.maxFramePayload ?? 64 * 1024;
    this.openTimeoutMs = opts.openTimeoutMs ?? 75_000;
    this.closeTimeoutMs = opts.closeTimeoutMs ?? 5 * 60_000;
    this.closed = new Promise((resolve) => (this.resolveClosed = resolve));
  }

  get isClosed(): boolean {
    return this.closeErr !== null;
  }

  get numStreams(): number {
    return this.streams.size;
  }

  /** Feeds bytes that arrived from the transport. */
  receive(chunk: Uint8Array): void {
    if (this.closeErr) return;
    this.rx.push(chunk);
    try {
      this.parse();
    } catch (e) {
      this.protocolError(e instanceof Error ? e : new Error(String(e)));
    }
  }

  /** Tells the session its transport is gone. */
  transportClosed(reason?: unknown): void {
    this.close(reason instanceof Error ? reason : new YamuxError("session_shutdown", "transport closed"));
  }

  /** Opens a stream. Waits while too many opens are unacknowledged. */
  async open(): Promise<Stream> {
    this.checkCanOpen();
    while (this.inflight.size >= this.acceptBacklog) {
      await this.inflightSignal.wait();
      this.checkCanOpen();
    }

    const id = this.nextStreamId;
    if (id >= 0xffffffff - 1) throw new YamuxError("streams_exhausted");
    this.nextStreamId += 2;

    const stream = new Stream(this, id, State.Init);
    this.streams.set(id, stream);
    let timer: Timer | undefined;
    if (this.openTimeoutMs > 0) {
      // The whole session, not just the stream: a peer that cannot ACK is a
      // peer that has gone, and reconnecting is the cure.
      timer = setTimeout(() => {
        if (this.inflight.has(id)) this.close(new YamuxError("timeout", "stream open timed out waiting for ACK"));
      }, this.openTimeoutMs);
    }
    this.inflight.set(id, timer);

    stream.sendWindowUpdate(); // carries the SYN
    return stream;
  }

  /** Waits for the peer to open a stream, and acknowledges it. */
  async accept(): Promise<Stream> {
    for (;;) {
      const stream = this.acceptQueue.shift();
      if (stream) {
        stream.sendWindowUpdate(); // carries the ACK
        return stream;
      }
      if (this.closeErr) throw this.closeErr;
      await this.acceptSignal.wait();
    }
  }

  /** Accepts streams until the session ends. */
  async *[Symbol.asyncIterator](): AsyncGenerator<Stream> {
    for (;;) {
      try {
        yield await this.accept();
      } catch (e) {
        if (this.closeErr) return;
        throw e;
      }
    }
  }

  /** Measures one round trip, in milliseconds. */
  async ping(): Promise<number> {
    if (this.closeErr) throw this.closeErr;
    const id = this.nextPingId;
    this.nextPingId = (this.nextPingId + 1) >>> 0;
    const done = new Promise<void>((resolve, reject) => this.pings.set(id, { resolve, reject }));
    const start = Date.now();
    this.sendFrame(TYPE_PING, FLAG_SYN, 0, null, id);
    await done;
    return Date.now() - start;
  }

  /** Refuses new streams from the peer from now on; existing ones carry on. */
  goAway(): void {
    this.localGoAway = true;
    this.sendFrame(TYPE_GO_AWAY, 0, 0, null, GO_AWAY_NORMAL);
  }

  /**
   * Ends the session and every stream on it. Streams still expecting data
   * fail with reason; a stream the peer had already finished keeps what it
   * has buffered and ends cleanly.
   */
  close(reason: Error = new YamuxError("session_shutdown")): void {
    if (this.closeErr) return;
    this.closeErr = reason;

    for (const timer of this.inflight.values()) if (timer !== undefined) clearTimeout(timer);
    this.inflight.clear();
    try {
      this.transport.close();
    } catch {
      // already gone
    }
    for (const stream of this.streams.values()) stream.forceClose(reason);
    this.streams.clear();
    this.acceptQueue = [];
    for (const { reject } of this.pings.values()) reject(reason);
    this.pings.clear();

    this.inflightSignal.notify();
    this.acceptSignal.notify();
    this.resolveClosed(reason);
  }

  /** @internal */
  sendFrame(type: number, flags: number, id: number, body: Uint8Array | null, length: number): void {
    if (this.closeErr) throw this.closeErr;
    const frame = new Uint8Array(HEADER_SIZE + (body?.length ?? 0));
    const view = new DataView(frame.buffer);
    frame[0] = PROTO_VERSION;
    frame[1] = type;
    view.setUint16(2, flags);
    view.setUint32(4, id);
    view.setUint32(8, length);
    if (body) frame.set(body, HEADER_SIZE);
    try {
      this.transport.send(frame);
    } catch (e) {
      const err = new YamuxError("session_shutdown", `transport send failed: ${e}`);
      this.close(err);
      throw err;
    }
  }

  /** @internal Forgets a stream both ends are done with. */
  closeStream(id: number): void {
    this.streams.delete(id);
    this.releaseInflight(id);
  }

  /** @internal Marks an opened stream as acknowledged. */
  establishStream(id: number): void {
    this.releaseInflight(id);
  }

  private releaseInflight(id: number): void {
    if (!this.inflight.has(id)) return;
    const timer = this.inflight.get(id);
    if (timer !== undefined) clearTimeout(timer);
    this.inflight.delete(id);
    this.inflightSignal.notify();
  }

  private checkCanOpen(): void {
    if (this.closeErr) throw this.closeErr;
    if (this.remoteGoAway) throw new YamuxError("remote_go_away");
  }

  // parse consumes every whole frame in rx, and returns to wait to be fed
  // when what is left is not a whole frame yet.
  private parse(): void {
    while (!this.closeErr) {
      const pending = this.pending;
      if (pending) {
        const take = Math.min(pending.remaining, this.rx.length);
        if (take === 0) return;
        const part = this.rx.read(take);
        pending.remaining -= take;
        if (pending.stream) pending.parts.push(part);
        if (pending.remaining > 0) return;
        this.pending = null;
        pending.stream?.readData(pending.parts, pending.flags);
        continue;
      }

      if (this.rx.length < HEADER_SIZE) return;
      const hdr = this.rx.read(HEADER_SIZE);
      const view = new DataView(hdr.buffer, hdr.byteOffset, HEADER_SIZE);
      if (hdr[0] !== PROTO_VERSION) throw new YamuxError("invalid_version", `invalid protocol version ${hdr[0]}`);
      const type = hdr[1]!;
      const flags = view.getUint16(2);
      const id = view.getUint32(4);
      const length = view.getUint32(8);

      switch (type) {
        case TYPE_DATA:
        case TYPE_WINDOW_UPDATE:
          this.handleStreamMessage(type, flags, id, length);
          break;
        case TYPE_PING:
          this.handlePing(flags, length);
          break;
        case TYPE_GO_AWAY:
          this.handleGoAway(length);
          break;
        default:
          throw new YamuxError("invalid_msg_type", `invalid msg type ${type}`);
      }
    }
  }

  private handleStreamMessage(type: number, flags: number, id: number, length: number): void {
    if (flags & FLAG_SYN) this.incomingStream(id);

    // No stream most likely means this end reset it; what is still on the
    // wire for it is drained, not an error.
    const stream = this.streams.get(id) ?? null;

    if (type === TYPE_WINDOW_UPDATE) {
      stream?.incrSendWindow(flags, length);
      return;
    }

    if (stream && length > stream.recvWindow) {
      throw new YamuxError(
        "recv_window_exceeded",
        `receive window exceeded (stream: ${id}, remain: ${stream.recvWindow}, recv: ${length})`,
      );
    }
    if (length === 0) {
      stream?.readData([], flags);
      return;
    }
    // The window is checked here and charged when the payload is complete,
    // and flags are applied after the data rather than before it: a payload
    // may arrive in pieces, and applying a FIN first would let a reader see
    // end-of-stream ahead of the data the FIN was sent with.
    this.pending = { stream, flags, remaining: length, parts: [] };
  }

  private incomingStream(id: number): void {
    if (this.localGoAway) {
      this.sendFrame(TYPE_WINDOW_UPDATE, FLAG_RST, id, null, 0);
      return;
    }
    if (this.streams.has(id)) throw new YamuxError("duplicate_stream", "duplicate stream initiated");
    if (this.acceptQueue.length >= this.acceptBacklog) {
      // Backlog exceeded: refuse this one, keep the session.
      this.sendFrame(TYPE_WINDOW_UPDATE, FLAG_RST, id, null, 0);
      return;
    }
    const stream = new Stream(this, id, State.SYNReceived);
    this.streams.set(id, stream);
    this.acceptQueue.push(stream);
    this.acceptSignal.notify();
  }

  private handlePing(flags: number, opaque: number): void {
    if (flags & FLAG_SYN) {
      this.sendFrame(TYPE_PING, FLAG_ACK, 0, null, opaque);
      return;
    }
    const waiter = this.pings.get(opaque);
    if (waiter) {
      this.pings.delete(opaque);
      waiter.resolve();
    }
  }

  private handleGoAway(code: number): void {
    switch (code) {
      case GO_AWAY_NORMAL:
        this.remoteGoAway = true;
        this.inflightSignal.notify(); // blocked opens should fail, not wait
        return;
      case GO_AWAY_PROTO_ERR:
        this.close(new YamuxError("protocol_error", "remote: yamux protocol error"));
        return;
      case GO_AWAY_INTERNAL_ERR:
        this.close(new YamuxError("protocol_error", "remote yamux internal error"));
        return;
      default:
        this.close(new YamuxError("protocol_error", `unexpected go away ${code}`));
    }
  }

  private protocolError(err: Error): void {
    try {
      this.sendFrame(TYPE_GO_AWAY, 0, 0, null, GO_AWAY_PROTO_ERR);
    } catch {
      // the session is going anyway
    }
    this.close(err);
  }
}

enum State {
  Init,
  SYNSent,
  SYNReceived,
  Established,
  LocalClose,
  RemoteClose,
  Closed,
  Reset,
}

/**
 * Stream is one yamux stream as a pair of Web Streams.
 *
 * Closing the writable sends FIN and nothing else: the readable goes on
 * delivering until the peer finishes too. That is the half-close vtunnel
 * depends on.
 * Aborting the writable, or calling reset, sends RST.
 */
export class Stream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;

  /** @internal */ recvWindow = INITIAL_STREAM_WINDOW;
  private sendWindow = INITIAL_STREAM_WINDOW;
  private state: State;

  private recvBuf: Uint8Array[] = [];
  private recvLen = 0;
  private readCancelled = false;
  private readonly recvSignal = new Signal();
  private readonly sendSignal = new Signal();

  // Set when the session died under a stream that still expected data.
  private sessionErr: Error | null = null;
  private closeTimer: Timer | undefined;

  /** @internal */
  constructor(
    private readonly session: Session,
    readonly id: number,
    state: State,
  ) {
    this.state = state;
    this.readable = new ReadableStream<Uint8Array>(
      {
        pull: (controller) => this.pull(controller),
        cancel: () => this.cancelRead(),
      },
      // Nothing is pulled ahead of the reader, so what sits unread is
      // exactly recvBuf, and the window the peer sees is the truth.
      { highWaterMark: 0 },
    );
    this.writable = new WritableStream<Uint8Array>({
      write: (chunk) => this.write(chunk),
      close: () => this.closeWrite(),
      abort: () => this.reset(),
    });
  }

  /** Sends FIN: this end will write nothing more. Reading carries on. */
  closeWrite(): void {
    let finished = false;
    switch (this.state) {
      case State.SYNSent:
      case State.SYNReceived:
      case State.Established:
        this.state = State.LocalClose;
        break;
      case State.RemoteClose:
        this.state = State.Closed;
        finished = true;
        break;
      default:
        return;
    }

    if (this.closeTimer !== undefined) clearTimeout(this.closeTimer);
    this.closeTimer = undefined;
    // A peer that never answers our FIN with its own would hold this stream
    // forever. It gets closeTimeoutMs, then a reset.
    if (!finished && this.session.closeTimeoutMs > 0) {
      this.closeTimer = setTimeout(() => this.closeTimeout(), this.session.closeTimeoutMs);
    }

    // The flags are worked out after the state moved, so FIN goes alone: a
    // stream gets here only after its SYN or ACK has been sent.
    this.trySend(TYPE_WINDOW_UPDATE, this.sendFlags() | FLAG_FIN, null, 0);
    this.notifyWaiting();
    if (finished) this.session.closeStream(this.id);
  }

  /** Abandons the stream in both directions and tells the peer so. */
  reset(): void {
    if (this.state === State.Reset || this.state === State.Closed) return;
    this.state = State.Reset;
    this.clearCloseTimer();
    this.trySend(TYPE_WINDOW_UPDATE, FLAG_RST, null, 0);
    this.notifyWaiting();
    this.session.closeStream(this.id);
  }

  private async pull(controller: ReadableStreamDefaultController<Uint8Array>): Promise<void> {
    for (;;) {
      if (this.state === State.Reset) throw new YamuxError("connection_reset");
      const chunk = this.recvBuf.shift();
      if (chunk) {
        this.recvLen -= chunk.length;
        controller.enqueue(chunk);
        this.trySendWindowUpdate();
        return;
      }
      if (this.state === State.RemoteClose || this.state === State.Closed) {
        if (this.sessionErr) throw this.sessionErr;
        controller.close();
        return;
      }
      await this.recvSignal.wait();
    }
  }

  // A cancelled reader still has a peer that may be mid-write. Dropping the
  // bytes while still granting window lets that peer finish instead of
  // stalling forever on a window nobody will open; reset is there for
  // whoever wants it stopped.
  private cancelRead(): void {
    this.readCancelled = true;
    this.recvBuf = [];
    this.recvLen = 0;
    this.trySendWindowUpdate();
  }

  private async write(b: Uint8Array): Promise<void> {
    let off = 0;
    while (off < b.length) {
      if (this.sessionErr) throw this.sessionErr;
      switch (this.state) {
        case State.LocalClose:
        case State.Closed:
          throw new YamuxError("stream_closed");
        case State.Reset:
          throw new YamuxError("connection_reset");
      }
      if (this.sendWindow === 0) {
        await this.sendSignal.wait();
        continue;
      }
      const n = Math.min(this.sendWindow, b.length - off, this.session.maxFramePayload);
      this.session.sendFrame(TYPE_DATA, this.sendFlags(), this.id, b.subarray(off, off + n), n);
      this.sendWindow -= n;
      off += n;
    }
  }

  private sendFlags(): number {
    switch (this.state) {
      case State.Init:
        this.state = State.SYNSent;
        return FLAG_SYN;
      case State.SYNReceived:
        this.state = State.Established;
        return FLAG_ACK;
      default:
        return 0;
    }
  }

  /** @internal Tops the peer's window back up to maxStreamWindow, if worth it. */
  sendWindowUpdate(): void {
    if (this.state === State.Reset || this.state === State.Closed) return;
    const max = this.session.maxStreamWindow;
    const delta = Math.max(0, max - this.recvLen - this.recvWindow);
    const flags = this.sendFlags();
    if (delta < max / 2 && flags === 0) return;
    this.recvWindow += delta;
    this.session.sendFrame(TYPE_WINDOW_UPDATE, flags, this.id, null, delta);
  }

  private trySendWindowUpdate(): void {
    try {
      this.sendWindowUpdate();
    } catch {
      // Session gone; the reader finds out on its next pull.
    }
  }

  private trySend(type: number, flags: number, body: Uint8Array | null, length: number): void {
    try {
      this.session.sendFrame(type, flags, this.id, body, length);
    } catch {
      // Session gone; nothing left to tell.
    }
  }

  /** @internal */
  readData(parts: Uint8Array[], flags: number): void {
    // Reset here while its payload was still arriving: nothing to deliver to,
    // and its flags describe a stream that no longer exists.
    if (this.state === State.Reset) return;
    let length = 0;
    for (const part of parts) length += part.length;
    this.recvWindow -= length;
    if (this.readCancelled) {
      if (length > 0) this.trySendWindowUpdate();
    } else {
      for (const part of parts) this.recvBuf.push(part);
      this.recvLen += length;
    }
    this.processFlags(flags);
    this.recvSignal.notify();
  }

  /** @internal */
  incrSendWindow(flags: number, length: number): void {
    if (this.state === State.Reset) return;
    this.processFlags(flags);
    this.sendWindow += length;
    this.sendSignal.notify();
  }

  private processFlags(flags: number): void {
    let finished = false;
    if (flags & FLAG_ACK) {
      if (this.state === State.SYNSent) this.state = State.Established;
      this.session.establishStream(this.id);
    }
    if (flags & FLAG_FIN) {
      switch (this.state) {
        case State.SYNSent:
        case State.SYNReceived:
        case State.Established:
          this.state = State.RemoteClose;
          break;
        case State.LocalClose:
          this.state = State.Closed;
          finished = true;
          break;
        default:
          throw new YamuxError("unexpected_flag", `unexpected FIN flag in state ${this.state}`);
      }
    }
    if (flags & FLAG_RST) {
      this.state = State.Reset;
      finished = true;
    }
    if (finished) {
      this.clearCloseTimer();
      this.session.closeStream(this.id);
    }
    this.notifyWaiting();
  }

  private closeTimeout(): void {
    this.closeTimer = undefined;
    this.forceClose(new YamuxError("timeout", "stream close timed out waiting for FIN"));
    this.session.closeStream(this.id);
    this.trySend(TYPE_WINDOW_UPDATE, FLAG_RST, null, 0);
  }

  /** @internal The session is going; so is this stream. */
  forceClose(reason: Error): void {
    switch (this.state) {
      case State.RemoteClose:
      case State.Closed:
      case State.Reset:
        break; // the peer had finished; what is buffered is still good
      default:
        this.sessionErr = reason;
    }
    if (this.state !== State.Reset) this.state = State.Closed;
    this.clearCloseTimer();
    this.notifyWaiting();
  }

  private clearCloseTimer(): void {
    if (this.closeTimer !== undefined) clearTimeout(this.closeTimer);
    this.closeTimer = undefined;
  }

  private notifyWaiting(): void {
    this.recvSignal.notify();
    this.sendSignal.notify();
  }
}
