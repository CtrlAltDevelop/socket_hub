/// One WebSocket, many channels.
///
/// A [SocketChannelHub] owns a single connection and hands out a stream per
/// subscription. It counts how many callers want each one, sends a subscribe
/// frame when the count reaches one and an unsubscribe when it falls to zero,
/// batches everything asked for in the same turn of the event loop into one
/// frame, and re-sends every live subscription after a reconnect. Streams
/// survive the reconnect — a drop is a gap in events, not a done event.
///
/// What the server's frames look like is a [SocketCodec]'s business.
/// [JsonSocketCodec] already speaks the widespread
/// `{"op": "subscribe", "args": [...]}` convention, so a host on that protocol
/// supplies a parser per channel and nothing more.
///
/// ```dart
/// final hub = SocketChannelHub<Object?>(
///   transport: () => WebSocketTransport.connect(Uri.parse('wss://…/stream')),
///   codec: JsonSocketCodec<Object?>(
///     parsers: {'ticker': Ticker.fromJson, 'candle': Candle.fromJson},
///     channelKeyFields: {'candle': {'symbol', 'interval'}},
///   ),
///   retainLatest: true,
///   heartbeatInterval: const Duration(seconds: 20),
///   idleTimeout: const Duration(seconds: 60),
/// );
///
/// // Subscribes on listen, unsubscribes on cancel, in one frame each.
/// final ticker = hub.stream(SubscriptionKey('ticker', {'symbol': 'BTCUSDT'}));
/// final candles = hub.stream(
///   SubscriptionKey('candle', {'symbol': 'BTCUSDT', 'interval': '15m'}),
/// );
/// ```
///
/// Pure Dart, so it works in Flutter apps, server code and CLIs alike. For
/// tests and mock modes, `package:socket_hub/socket_hub_testing.dart`
/// brings a fake transport and a stand-in server.
library;

export 'src/connection_state.dart';
export 'src/json_socket_codec.dart';
export 'src/reconnect_policy.dart';
export 'src/socket_channel_hub.dart';
export 'src/socket_codec.dart';
export 'src/subscription_key.dart';
export 'src/transport.dart';
