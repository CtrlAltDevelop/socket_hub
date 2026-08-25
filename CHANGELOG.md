# Changelog

## 1.0.0

Initial release, extracted from three near-identical WebSocket data sources in
an internal trading app and generalised on the way out. Several things the
originals got wrong are fixed here rather than carried over.

- `SocketChannelHub` — one socket, a stream per subscription. Reference counts
  come from stream listeners, so listening subscribes and cancelling the last
  listener unsubscribes; there is no `subscribe`/`unsubscribe` pair for a caller
  to keep balanced.
- **Subscriptions are batched per microtask.** A screen opening four streams
  sends one frame, and a symbol switch that closes four and opens four sends
  two. The originals sent one frame per channel, and had a separate
  `subscribeTickerList` method purely to batch a list of symbols by hand — that
  method has no successor because it is no longer needed.
- **A subscribe and an unsubscribe of the same key in one turn cancel out.**
  Reconciling from the difference between what callers want and what the server
  has been told, rather than from a queue of deltas, means a stream opened and
  closed in the same turn touches the wire not at all.
- **Reconnection, which the originals had none of.** They called
  `listen(cancelOnError: true)` and closed every stream controller on the first
  socket error, so one dropped frame ended every subscription in the app and
  left the sockets shut until something happened to rebuild them. Here a drop is
  a gap in events, streams stay open, and every live subscription is re-sent on
  the new socket. `ReconnectPolicy` covers the backoff, with jitter so a fleet
  dropped by one outage does not return in lockstep.
- **A keepalive.** `heartbeatInterval` sends the codec's ping frame, and
  `idleTimeout` treats a socket that has delivered nothing for that long as
  dead and reopens it. Neither existed before, which is the other half of why a
  silently dropped connection went unnoticed. The two run on separate timers,
  so a protocol needing no ping can still have its dead sockets noticed.
- **The payload cache is bounded.** The originals created a stream controller
  for every key an inbound frame mentioned, whether or not anyone had asked for
  it, so a server pushing hundreds of symbols grew the map for the life of the
  session. Nothing is now created or retained for a key no caller holds.
- `SocketCodec` — the whole protocol behind four methods, with `handshake`
  running after the socket opens and before any subscription frame, so a login
  gating private channels is ordered by construction rather than by a
  `_pendingPrivateSubscribe` flag.
- `JsonSocketCodec` — a ready-made codec for the
  `{"op": "subscribe", "args": [...]}` convention, reading a frame's key fields
  from the top level, from a nested `args` map or from inside `data`. All three
  of the originals' wire shapes go through it with configuration alone.
- `SubscriptionKey` — a channel plus arguments, with a canonical id, so
  argument order cannot split one subscription into two. Null arguments are
  dropped, which removes the conditionals the originals built keys with.
- Fan-out is declarative: a codec returns more than one key and the payload is
  routed to each. The originals hard-coded a `'::ALL:'` key prefix and a
  per-channel `isAccount` check in the middle of the message handler.
- `SocketTransport` — the socket behind an interface, with `WebSocketTransport`
  over `package:web_socket_channel`. The originals reached for
  `WebSocketChannel.connect` directly and so could not be tested at all.
- `socket_channels_testing.dart` — `FakeTransport`, a socket with nothing
  behind it, and `MockSocketServer`, which reads the frames a hub sends and
  answers them. Mock mode becomes a different far end rather than a parallel set
  of data sources.
- **`ReconnectPolicy` computes its backoff in doubles.** `pow(2, attempt)` on
  two ints does integer arithmetic, which wraps to zero past 2⁶³ — so a long
  outage would have produced a zero delay and a reconnect storm. Caught by a
  test asserting the delay stops growing at `maxDelay`.
- **`disconnect()` clears the failure flag it left behind.** That flag stops
  one dying socket reporting itself three times from scheduling three
  reconnects. Left set by a disconnect that landed mid-backoff, it also
  swallowed the failure of the *next* attempt — so an app backgrounded during a
  retry and resumed into a still-unreachable server sat in `connecting` with
  nothing on a timer, needing a restart to recover.
- Dartdoc across the public API, a runnable `example/` that needs no network,
  and 65 tests covering reference counting, batching, routing, fan-out,
  reconnection, the handshake, the keepalive and the lifecycle.
