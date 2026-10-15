import 'dart:async';

import 'package:web_socket_channel/web_socket_channel.dart';

/// The socket a hub talks through.
///
/// Everything the hub needs from a connection is behind this interface, so a
/// test can drive the hub with `FakeTransport` from
/// `package:socket_hub/socket_hub_testing.dart` instead of a server,
/// and mock mode can feed it generated data.
abstract interface class SocketTransport {
  /// Frames arriving from the far end, exactly as the socket delivers them —
  /// usually [String], sometimes `List<int>`.
  ///
  /// A single-subscription stream: the hub is the only listener.
  Stream<dynamic> get incoming;

  /// Completes when the socket is open, or with an error if it never opens.
  Future<void> get ready;

  /// Sends one frame. Called only after [ready] completes.
  void send(Object data);

  /// Closes the socket. Must complete even if it was never open.
  Future<void> close();
}

/// Opens a new transport. Called once per connection attempt, so each
/// reconnect gets a fresh socket rather than a reused one.
typedef SocketTransportFactory = SocketTransport Function();

/// Where [WebSocketTransport.connect] reports a `ws://` URL. Defaults to
/// [print] if not overridden; wire it to the hub's own `SocketLogger` to put
/// the warning where the rest of the hub's logging goes:
///
/// ```dart
/// transport: () => WebSocketTransport.connect(
///   url,
///   onInsecure: (message) => log(message),
/// ),
/// ```
typedef SocketInsecureWarning = void Function(String message);

/// The warning [WebSocketTransport.connect] would raise for [url], or null
/// if its scheme is `wss`.
///
/// A plain function rather than inlined in `connect`, so what counts as
/// insecure and what the warning says can be checked without opening a real
/// socket.
String? insecureWebSocketWarning(Uri url) {
  if (url.scheme == 'wss') return null;
  return 'Connecting to "$url" over "${url.scheme}", not "wss" — this '
      'connection is not encrypted. Use a wss:// URL if the server supports '
      'it.';
}

/// A [SocketTransport] backed by `package:web_socket_channel`.
///
/// ```dart
/// SocketChannelHub<Object?>(
///   transport: () => WebSocketTransport.connect(Uri.parse('wss://…/stream')),
///   codec: myCodec,
/// );
/// ```
class WebSocketTransport implements SocketTransport {
  /// Wraps an already-created [channel].
  ///
  /// [maxIncomingBytes] rejects an inbound frame larger than that many bytes
  /// — a [String] frame's UTF-16 code units, or a binary frame's byte length
  /// — as a socket error, which the hub treats like any other dropped
  /// connection: it tears down and reconnects. Left null, any frame size is
  /// accepted. Guards against a misbehaving or compromised server pushing an
  /// oversized frame to force a large decode (or a large retained payload) on
  /// every client connected to it.
  WebSocketTransport(this.channel, {this.maxIncomingBytes});

  /// Opens a socket to [url], offering [protocols] as sub-protocols.
  ///
  /// [url] is not required to be `wss://` — a plain `ws://` connection still
  /// works, for a local dev server or a host with no TLS — but every byte on
  /// it is unencrypted and unauthenticated, so [onInsecure] is called with a
  /// one-line warning when the scheme isn't `wss`. Left unset, the warning
  /// goes to [print]; pass the hub's own `SocketLogger` to route it wherever
  /// the rest of the hub's logging goes, or a no-op to silence it once the
  /// warning has been seen and accepted.
  ///
  /// For a keepalive, set `heartbeatInterval` on the hub and give the codec an
  /// `encodeHeartbeat` frame. That is an application-level ping, so it works
  /// on the web too — where the browser owns the socket and a protocol-level
  /// ping is not ours to send.
  ///
  /// See the constructor for [maxIncomingBytes].
  factory WebSocketTransport.connect(
    Uri url, {
    Iterable<String>? protocols,
    int? maxIncomingBytes,
    SocketInsecureWarning? onInsecure,
  }) {
    final String? warning = insecureWebSocketWarning(url);
    if (warning != null) (onInsecure ?? print)(warning);
    return WebSocketTransport(
      WebSocketChannel.connect(url, protocols: protocols),
      maxIncomingBytes: maxIncomingBytes,
    );
  }

  /// The underlying channel.
  final WebSocketChannel channel;

  /// Rejects an inbound frame larger than this many bytes, or null to accept
  /// any size. See the constructor.
  final int? maxIncomingBytes;

  @override
  Stream<dynamic> get incoming {
    final int? limit = maxIncomingBytes;
    if (limit == null) return channel.stream;
    return channel.stream.map((dynamic frame) {
      final int size = switch (frame) {
        final String s => s.length,
        final List<int> bytes => bytes.length,
        _ => 0,
      };
      if (size > limit) {
        throw SocketFrameTooLargeException(size, limit);
      }
      return frame;
    });
  }

  @override
  Future<void> get ready => channel.ready;

  @override
  void send(Object data) => channel.sink.add(data);

  @override
  Future<void> close() async {
    // A sink whose socket never opened throws on close; the caller is tearing
    // the connection down either way, so there is nothing to report.
    try {
      await channel.sink.close();
    } on Object {
      // Ignored deliberately.
    }
  }
}

/// Thrown when an inbound frame exceeds a [WebSocketTransport]'s
/// `maxIncomingBytes`. The hub treats it like any other socket error: the
/// connection is torn down and the reconnect policy decides what happens
/// next.
class SocketFrameTooLargeException implements Exception {
  /// Creates an exception reporting a frame of [sizeBytes] against a
  /// [limitBytes] ceiling.
  const SocketFrameTooLargeException(this.sizeBytes, this.limitBytes);

  /// The size of the frame that was rejected.
  final int sizeBytes;

  /// The ceiling it exceeded.
  final int limitBytes;

  @override
  String toString() =>
      'SocketFrameTooLargeException: $sizeBytes bytes exceeds the '
      '$limitBytes byte limit';
}
