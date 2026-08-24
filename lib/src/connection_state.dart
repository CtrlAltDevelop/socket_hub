/// Where a `SocketChannelHub` is in its connection lifecycle.
///
/// Read it from `SocketChannelHub.connectionStates` to drive a banner, or
/// gate a "place order" button on [SocketConnectionState.ready].
enum SocketConnectionState {
  /// No socket, and none being opened. The state a hub starts in, and the one
  /// it returns to after `disconnect()`.
  idle,

  /// A socket is being opened, or its handshake is still running.
  ///
  /// Subscriptions asked for in this state are held and sent once the socket
  /// is ready, so callers never have to wait for it.
  connecting,

  /// The socket is open, the handshake finished, and subscriptions are live.
  ready,

  /// The socket dropped and a reconnect is scheduled.
  ///
  /// Streams stay open across this — a listener sees a gap in events, not a
  /// done event — and every live subscription is re-sent once the socket
  /// comes back.
  reconnecting,

  /// The hub gave up: either the reconnect policy ran out of attempts, or
  /// `dispose()` was called. Terminal — a disposed hub cannot be reused.
  closed,
}

/// Whether a hub in this state can carry frames right now.
extension SocketConnectionStateX on SocketConnectionState {
  /// True only in [SocketConnectionState.ready].
  bool get isReady => this == SocketConnectionState.ready;

  /// True while a socket is being established or re-established.
  bool get isConnecting =>
      this == SocketConnectionState.connecting ||
      this == SocketConnectionState.reconnecting;

  /// True once the hub is finished for good.
  bool get isClosed => this == SocketConnectionState.closed;
}
