import 'package:socket_hub/socket_hub.dart';
import 'package:socket_hub/socket_hub_testing.dart';
import 'package:test/test.dart';

Future<void> settle([int turns = 6]) async {
  for (int i = 0; i < turns; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}

typedef Payload = Map<String, Object?>;

void main() {
  /// Long enough that only [MockSocketServer.pump] drives the feed.
  const Duration manual = Duration(days: 1);

  late MockSocketServer server;
  late SocketChannelHub<Payload> hub;

  setUp(() {
    server = MockSocketServer(
      tick: manual,
      build: (SubscriptionKey key, int tick) => switch (key.channel) {
        'ticker' => <String, Object?>{
          'symbol': key.args['symbol'],
          'last': 100 + tick,
        },
        'candle' => <String, Object?>{
          'interval': key.args['interval'],
          'close': 100 + tick,
        },
        _ => null,
      },
    );
    hub = SocketChannelHub<Payload>(
      transport: server.open,
      codec: JsonSocketCodec<Payload>(
        parsers: <String, JsonPayloadParser<Payload>>{
          'ticker': (Object? d) => (d! as Map).cast<String, Object?>(),
          'candle': (Object? d) => (d! as Map).cast<String, Object?>(),
        },
        channelKeyFields: <String, Set<String>>{
          'candle': <String>{'symbol', 'interval'},
        },
      ),
      reconnectPolicy: const ReconnectPolicy(
        initialDelay: Duration(milliseconds: 1),
        jitter: 0,
      ),
    );
  });

  tearDown(() async {
    await hub.dispose();
    await server.dispose();
  });

  test('the server sees what the hub subscribed to', () async {
    hub
        .stream(SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}))
        .listen((_) {});
    hub
        .stream(
          SubscriptionKey('candle', <String, String>{
            'symbol': 'BTC',
            'interval': '15m',
          }),
        )
        .listen((_) {});
    await hub.whenReady();
    await settle();

    expect(
      server.subscriptions.map((SubscriptionKey k) => k.id).toSet(),
      <String>{'ticker|symbol=BTC', 'candle|interval=15m|symbol=BTC'},
    );
  });

  test('a pump reaches the stream through the real codec', () async {
    final List<Payload> ticks = <Payload>[];
    hub
        .stream(SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}))
        .listen(ticks.add);
    await hub.whenReady();
    await settle();

    server.pump();
    server.pump();
    await settle();

    expect(ticks, <Payload>[
      <String, Object?>{'symbol': 'BTC', 'last': 100},
      <String, Object?>{'symbol': 'BTC', 'last': 101},
    ]);
  });

  test('a channel the builder has nothing for stays quiet', () async {
    final List<Payload> ticks = <Payload>[];
    hub
        .stream(SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}))
        .listen(ticks.add);
    // 'depth' has no parser and no builder entry: subscribed, but silent.
    hub
        .stream(SubscriptionKey('depth', <String, String>{'symbol': 'BTC'}))
        .listen((_) {});
    await hub.whenReady();
    await settle();

    server.pump();
    await settle();

    expect(ticks, hasLength(1));
  });

  test('cancelling stops the feed for that key alone', () async {
    final List<Payload> btc = <Payload>[];
    final List<Payload> eth = <Payload>[];
    final subscription = hub
        .stream(SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}))
        .listen(btc.add);
    hub
        .stream(SubscriptionKey('ticker', <String, String>{'symbol': 'ETH'}))
        .listen(eth.add);
    await hub.whenReady();
    await settle();

    await subscription.cancel();
    await settle();
    server.pump();
    await settle();

    expect(btc, isEmpty);
    expect(eth, hasLength(1));
    expect(server.subscriptions, hasLength(1));
  });

  test('a dropped socket comes back with the subscriptions intact', () async {
    final List<Payload> ticks = <Payload>[];
    hub
        .stream(SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}))
        .listen(ticks.add);
    await hub.whenReady();
    await settle();

    await server.transport!.closeByPeer();
    await Future<void>.delayed(const Duration(milliseconds: 30));
    await settle();

    expect(hub.connectionState, SocketConnectionState.ready);
    expect(server.subscriptions, hasLength(1));

    server.pump();
    await settle();
    expect(ticks, hasLength(1), reason: 'the same stream, a new socket');
  });

  test('acknowledges control frames so a handshake can complete', () async {
    final List<SocketControl<Object?>> controls = <SocketControl<Object?>>[];
    hub.controlFrames.listen(controls.add);

    hub
        .stream(SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}))
        .listen((_) {});
    await hub.whenReady();
    await settle();

    expect(
      controls.map((SocketControl<Object?> c) => c.op),
      contains('subscribe'),
    );
    expect(controls.every((SocketControl<Object?> c) => !c.isError), isTrue);
  });
}
