// A market-data socket, end to end, with no network involved: the far end is
// MockSocketServer from the testing library, so this file runs as it stands.
//
//   dart run example/socket_hub_example.dart
//
// Swap `transport: server.open` for
// `transport: () => WebSocketTransport.connect(url)` and the rest is unchanged.
import 'dart:convert';

import 'package:socket_hub/socket_hub.dart';
import 'package:socket_hub/socket_hub_testing.dart';

/// The channels this server publishes.
///
/// Not required by the package — a channel is just a string — but an enum
/// keeps the call sites honest, and gives one place to spell the wire names.
enum Channel {
  ticker('ticker'),
  candle('candle'),
  orders('account_orders');

  const Channel(this.wire);

  final String wire;

  SubscriptionKey of({String? symbol, String? interval}) =>
      // Null args are dropped by SubscriptionKey, so both can go in as they
      // are — `of()` with neither is the bare channel.
      SubscriptionKey(wire, <String, String?>{
        'symbol': symbol,
        'interval': interval,
      });

  /// Every order, whatever symbol it was for. See `fanOutChannels` below.
  SubscriptionKey get all => SubscriptionKey(wire);
}

/// One decoded frame. A real app would parse into model classes here.
typedef Payload = Map<String, Object?>;

Payload parse(Object? data) => (data! as Map).cast<String, Object?>();

/// The protocol: the `{"op": …, "args": [...]}` convention, plus a login the
/// private channels are gated behind.
class ExchangeCodec extends JsonSocketCodec<Payload> {
  ExchangeCodec(this._token)
      : super(
          parsers: <String, JsonPayloadParser<Payload>>{
            Channel.ticker.wire: parse,
            Channel.candle.wire: parse,
            Channel.orders.wire: parse,
          },
          // A candle frame carries its interval inside `data`, so the interval
          // has to be part of the key or two intervals would share a stream.
          channelKeyFields: <String, Set<String>>{
            Channel.candle.wire: <String>{'symbol', 'interval'},
          },
          // An order arrives for one symbol but a portfolio screen wants them
          // all, so every order is routed twice.
          fanOutChannels: <String>{Channel.orders.wire},
          heartbeatFrame: const <String, Object?>{'op': 'ping'},
        );

  final String? _token;

  @override
  Future<void> handshake(SocketHandshake socket) async {
    final String? token = _token;
    if (token == null) return; // public channels only
    socket.send(jsonEncode(<String, Object?>{'op': 'login', 'token': token}));
    // Throws if the server refuses, which fails the attempt and hands it to
    // the reconnect policy — the right move for an expired token.
    await socket.expect('login');
  }
}

Future<void> main() async {
  // The stand-in server. It reads the subscribe frames the hub sends and
  // answers whatever is subscribed.
  final MockSocketServer server = MockSocketServer(
    tick: const Duration(milliseconds: 120),
    build: (SubscriptionKey key, int tick) {
      final String symbol = key.args['symbol'] ?? 'BTCUSDT';
      return switch (key.channel) {
        'ticker' => <String, Object?>{'symbol': symbol, 'last': 64000 + tick},
        'candle' => <String, Object?>{
            'interval': key.args['interval'],
            'close': 64000 + tick,
          },
        'account_orders' => tick.isEven
            ? <String, Object?>{
                'symbol': symbol,
                'id': tick,
                'status': 'FILLED',
              }
            : null, // nothing to report this tick
        _ => null,
      };
    },
  );

  final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
    transport: server.open,
    codec: ExchangeCodec('a-token'),
    retainLatest: true,
    // Short, and with no jitter, only so the reconnect below is quick to
    // watch. Leave the default in a real app.
    reconnectPolicy: const ReconnectPolicy(
      initialDelay: Duration(milliseconds: 150),
      jitter: 0,
    ),
    heartbeatInterval: const Duration(seconds: 20),
    idleTimeout: const Duration(seconds: 60),
    log: (String message, {Object? error, StackTrace? stackTrace}) =>
        print('  [hub] $message${error == null ? '' : ' — $error'}'),
  );

  // 1. Listening is subscribing. Four streams opened in one turn of the event
  //    loop leave as one frame.
  print('Opening four streams…');
  final subscriptions = <String, Object?>{};
  hub
      .stream(Channel.ticker.of(symbol: 'BTCUSDT'))
      .listen((Payload t) => subscriptions['ticker BTC'] = t['last']);
  hub
      .stream(Channel.ticker.of(symbol: 'ETHUSDT'))
      .listen((Payload t) => subscriptions['ticker ETH'] = t['last']);
  hub
      .stream(Channel.candle.of(symbol: 'BTCUSDT', interval: '15m'))
      .listen((Payload c) => subscriptions['candle 15m'] = c['close']);
  hub
      .stream(Channel.orders.all)
      .listen((Payload o) => subscriptions['last order'] = o['id']);

  await hub.whenReady();
  await Future<void>.delayed(const Duration(milliseconds: 10));
  print(
    '  logged in, then subscribed: ${server.subscriptions.length} channels',
  );
  print('  frames sent so far: ${server.transport!.sent.length}');

  await Future<void>.delayed(const Duration(milliseconds: 400));
  print('After a few ticks: $subscriptions');

  // 2. A second listener on a live key costs nothing on the wire, and with
  //    retainLatest it starts with the value already in hand.
  print('\nA late listener gets the last value immediately:');
  hub
      .stream(Channel.ticker.of(symbol: 'BTCUSDT'))
      .take(1)
      .listen((Payload t) => print('  arrived at once: last=${t['last']}'));
  await Future<void>.delayed(const Duration(milliseconds: 10));

  // 3. The socket drops. The streams do not: they see a gap, and every
  //    subscription is re-sent on the new socket.
  print('\nDropping the socket…');
  await server.transport!.closeByPeer();
  await Future<void>.delayed(const Duration(milliseconds: 600));
  print('  state: ${hub.connectionState.name}');
  print('  resubscribed: ${server.subscriptions.length} channels');
  print('  still ticking: $subscriptions');

  await hub.dispose();
  await server.dispose();
}
