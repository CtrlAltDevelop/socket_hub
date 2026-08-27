import 'dart:async';

import 'subscription_key.dart';

/// What one inbound frame turned out to be.
///
/// Returned by [SocketCodec.decode]. The hub acts on the variant: it routes a
/// [SocketPayload] to streams, offers a [SocketControl] to whatever is waiting
/// on the handshake, and counts a [SocketIgnored] without doing anything else.
sealed class SocketDecoded<T> {
  const SocketDecoded();
}

/// A data frame, and the subscriptions it belongs to.
///
/// Returning more than one key fans one frame out to several streams — the
/// idiomatic way to serve both a per-symbol stream and an account-wide one
/// from a single account update, without the hub knowing anything about it:
///
/// ```dart
/// return SocketPayload(order, keys: [
///   SubscriptionKey('orders', {'symbol': symbol}),
///   SubscriptionKey('orders'),
/// ]);
/// ```
final class SocketPayload<T> extends SocketDecoded<T> {
  /// Routes [value] to every key in [keys].
  const SocketPayload(this.value, {required this.keys});

  /// Routes [value] to a single key.
  SocketPayload.single(this.value, SubscriptionKey key)
      : keys = <SubscriptionKey>[key];

  /// The decoded payload, as the host's own type.
  final T value;

  /// The subscriptions this payload belongs to. May be empty, which drops it.
  final List<SubscriptionKey> keys;
}

/// A control frame: a reply to a subscribe, a login result, a pong.
///
/// The hub does not interpret these beyond logging an [error]. They exist so
/// [SocketCodec.handshake] can wait for one, and so a host can watch
/// `SocketChannelHub.controlFrames` for protocol-level trouble.
final class SocketControl<T> extends SocketDecoded<T> {
  /// Creates a control frame result.
  const SocketControl(this.op, {this.error, this.frame});

  /// The operation this frame is about — `'login'`, `'subscribe'`, `'pong'`.
  final String op;

  /// The error the server reported, or null when the operation succeeded.
  final Object? error;

  /// The raw decoded frame, for a host that needs more than [op] and [error].
  final Object? frame;

  /// Whether the server reported a failure.
  bool get isError => error != null;

  @override
  String toString() =>
      'SocketControl($op${error == null ? '' : ', error: $error'})';
}

/// A frame the codec could not place. Counted and logged, then dropped.
final class SocketIgnored<T> extends SocketDecoded<T> {
  /// Creates an ignored-frame result, with a [reason] for the log.
  const SocketIgnored(this.reason, {this.frame});

  /// Why the frame was not routed, in a form worth logging.
  final String reason;

  /// The raw frame, so the log can name what arrived.
  final Object? frame;

  @override
  String toString() => 'SocketIgnored($reason)';
}

/// The channel a handshake talks through while a connection is being brought
/// up, before any subscription is sent.
///
/// Passed to [SocketCodec.handshake]. Frames sent here jump the queue: they go
/// out before the hub reconciles subscriptions, which is what makes it
/// possible to log in first and subscribe to private channels after.
abstract interface class SocketHandshake {
  /// Sends [frame] on the socket now.
  void send(Object frame);

  /// Every control frame arriving during the handshake.
  Stream<SocketControl<Object?>> get controlFrames;

  /// Waits for the first control frame whose op is [op].
  ///
  /// Throws a [SocketHandshakeException] if the frame reports an error, or if
  /// none arrives within [timeout].
  Future<SocketControl<Object?>> expect(
    String op, {
    Duration timeout = const Duration(seconds: 10),
  });
}

/// Thrown when a handshake cannot be completed.
///
/// The hub treats it like a dropped socket: the connection is torn down and
/// the reconnect policy decides whether to try again. That is the behaviour
/// you want for an expired token — retrying with a fresh one will work.
class SocketHandshakeException implements Exception {
  /// Creates a handshake failure described by [message].
  SocketHandshakeException(this.message, {this.cause});

  /// What went wrong, phrased for a log.
  final String message;

  /// The underlying error, when there was one.
  final Object? cause;

  @override
  String toString() =>
      'SocketHandshakeException: $message${cause == null ? '' : ' ($cause)'}';
}

/// Translates between a server's wire protocol and this package's model of
/// subscriptions and payloads.
///
/// The hub owns connection lifecycle, reference counting, batching and
/// reconnection; the codec owns everything protocol-specific. Implement it
/// once per server. For the widespread `{"op": "subscribe", "args": [...]}`
/// convention, `JsonSocketCodec` is already that implementation.
///
/// [T] is whatever the host wants off the socket: a `Map`, a sealed union of
/// model classes, or `Object?` for the undecided.
abstract class SocketCodec<T> {
  /// Allows const subclasses.
  const SocketCodec();

  /// The frame that subscribes to [keys], or null to send nothing.
  ///
  /// [keys] is never empty. It may hold keys on different channels — the hub
  /// batches everything asked for in one turn of the event loop into a single
  /// call, so a screen opening four streams produces one frame.
  Object? encodeSubscribe(List<SubscriptionKey> keys);

  /// The frame that unsubscribes from [keys], or null to send nothing.
  ///
  /// [keys] is never empty, and is batched the same way as [encodeSubscribe].
  Object? encodeUnsubscribe(List<SubscriptionKey> keys);

  /// Reads one inbound [frame], as the transport delivered it.
  ///
  /// Must not throw: return a [SocketIgnored] for anything unexpected. A
  /// codec that throws is treated as having returned [SocketIgnored], and the
  /// error is reported through the hub's logger.
  SocketDecoded<T> decode(Object? frame);

  /// Runs after the socket opens and before any subscription is sent.
  ///
  /// The default does nothing. Override it to log in, to authenticate, or to
  /// wait for a server hello:
  ///
  /// ```dart
  /// @override
  /// Future<void> handshake(SocketHandshake socket) async {
  ///   final token = await readToken();
  ///   if (token == null) return;              // public channels only
  ///   socket.send(jsonEncode({'op': 'login', 'token': token}));
  ///   await socket.expect('login');           // throws if it failed
  /// }
  /// ```
  ///
  /// Throwing — including [SocketHandshake.expect] timing out — fails the
  /// connection attempt and hands it to the reconnect policy.
  Future<void> handshake(SocketHandshake socket) async {}

  /// The keepalive frame, or null for a protocol that needs none.
  ///
  /// Sent every `SocketChannelHub.heartbeatInterval` while the socket is
  /// ready. Servers that drop idle connections need this; the reply usually
  /// comes back as a [SocketControl] the hub only has to see, not act on.
  Object? encodeHeartbeat() => null;
}
