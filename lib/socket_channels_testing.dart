/// Test doubles for `socket_channels`: a socket with no network behind it, and
/// a stand-in server that answers what a hub subscribes to.
///
/// Import this in tests, and in a mock mode where the real backend is not
/// available. Neither piece is needed in production, so keeping them in a
/// second library keeps them out of a release build's tree-shaken output.
///
/// ```dart
/// final server = MockSocketServer(
///   build: (key, tick) => {'symbol': key.args['symbol'], 'last': 100 + tick},
///   tick: const Duration(days: 1),        // driven by pump(), not a clock
/// );
/// final hub = SocketChannelHub<Object?>(transport: server.open, codec: codec);
///
/// final ticks = <Object?>[];
/// hub.stream(SubscriptionKey('ticker', {'symbol': 'BTC'})).listen(ticks.add);
/// await hub.whenReady();
/// server.pump();
/// ```
library;

export 'src/testing/fake_transport.dart';
export 'src/testing/mock_socket_server.dart';
