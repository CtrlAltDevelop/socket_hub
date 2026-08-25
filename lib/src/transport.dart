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
  WebSocketTransport(this.channel);

  /// Opens a socket to [url], offering [protocols] as sub-protocols.
  ///
  /// For a keepalive, set `heartbeatInterval` on the hub and give the codec an
  /// `encodeHeartbeat` frame. That is an application-level ping, so it works
  /// on the web too — where the browser owns the socket and a protocol-level
  /// ping is not ours to send.
  factory WebSocketTransport.connect(Uri url, {Iterable<String>? protocols}) =>
      WebSocketTransport(WebSocketChannel.connect(url, protocols: protocols));

  /// The underlying channel.
  final WebSocketChannel channel;

  @override
  Stream<dynamic> get incoming => channel.stream;

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
