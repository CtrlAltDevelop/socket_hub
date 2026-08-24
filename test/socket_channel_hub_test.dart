import 'dart:async';
import 'dart:convert';

import 'package:socket_channels/socket_channels.dart';
import 'package:socket_channels/socket_channels_testing.dart';
import 'package:test/test.dart';

/// Lets every pending microtask and zero-duration timer run.
Future<void> settle([int turns = 6]) async {
  for (int i = 0; i < turns; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}

/// The decoded `data` of a frame, which is all these tests need off the wire.
typedef Payload = Map<String, Object?>;

Payload parse(Object? data) => (data! as Map).cast<String, Object?>();

JsonSocketCodec<Payload> codec({
  Set<String> fanOut = const <String>{},
  Map<String, Object?>? heartbeat,
}) => JsonSocketCodec<Payload>(
  parsers: <String, JsonPayloadParser<Payload>>{
    'ticker': parse,
    'trades': parse,
    'orders': parse,
  },
  fanOutChannels: fanOut,
  heartbeatFrame: heartbeat,
);

/// The `args` of every subscribe/unsubscribe frame in [frames], flattened to
/// the channel/argument maps the server would see.
List<Map<String, Object?>> argsOf(Iterable<Object> frames, String op) =>
    <Map<String, Object?>>[
      for (final Object frame in frames)
        if (jsonDecode(frame as String) case final Map<String, Object?> json)
          if (json['op'] == op)
            for (final Object? entry in json['args']! as List<Object?>)
              (entry! as Map).cast<String, Object?>(),
    ];

SubscriptionKey ticker(String symbol) =>
    SubscriptionKey('ticker', <String, String>{'symbol': symbol});
SubscriptionKey trades(String symbol) =>
    SubscriptionKey('trades', <String, String>{'symbol': symbol});

void main() {
  group('reference counting', () {
    test('several streams opened in one turn cost a single frame', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
      );
      addTearDown(hub.dispose);

      hub.stream(ticker('BTC')).listen((_) {});
      hub.stream(ticker('ETH')).listen((_) {});
      hub.stream(trades('BTC')).listen((_) {});
      await hub.whenReady();
      await settle();

      expect(transport.sent, hasLength(1), reason: 'one batched frame');
      expect(argsOf(transport.sent, 'subscribe'), hasLength(3));
      expect(hub.wireSubscriptions, hasLength(3));
    });

    test('a second listener rides the first subscription', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
      );
      addTearDown(hub.dispose);

      final StreamSubscription<Payload> first = hub
          .stream(ticker('BTC'))
          .listen((_) {});
      await hub.whenReady();
      await settle();
      transport.takeSent();

      final StreamSubscription<Payload> second = hub
          .stream(ticker('BTC'))
          .listen((_) {});
      await settle();
      expect(transport.sent, isEmpty, reason: 'already subscribed');
      expect(hub.refCount(ticker('BTC')), 2, reason: 'each listener counts');

      await first.cancel();
      await settle();
      expect(transport.sent, isEmpty, reason: 'still one listener left');

      await second.cancel();
      await settle();
      expect(argsOf(transport.sent, 'unsubscribe'), hasLength(1));
      expect(hub.refCount(ticker('BTC')), 0);
    });

    test(
      'a subscribe cancelled in the same turn never reaches the wire',
      () async {
        final FakeTransport transport = FakeTransport();
        final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
          transport: () => transport,
          codec: codec(),
        );
        addTearDown(hub.dispose);

        await hub.connect();
        transport.takeSent();

        await hub.stream(ticker('BTC')).listen((_) {}).cancel();
        await settle();

        expect(transport.sent, isEmpty);
        expect(hub.wireSubscriptions, isEmpty);
      },
    );

    test('a lease holds a subscription open with nothing listening', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
      );
      addTearDown(hub.dispose);

      final SubscriptionLease lease = hub.subscribeAll(<SubscriptionKey>[
        ticker('BTC'),
        trades('BTC'),
      ]);
      await hub.whenReady();
      await settle();

      expect(argsOf(transport.sent, 'subscribe'), hasLength(2));
      expect(hub.refCount(ticker('BTC')), 1);

      transport.takeSent();
      lease.close();
      lease.close(); // idempotent
      await settle();

      expect(argsOf(transport.sent, 'unsubscribe'), hasLength(2));
      expect(lease.isClosed, isTrue);
      expect(hub.activeSubscriptions, isEmpty);
    });
  });

  group('routing', () {
    test('a payload reaches only the stream it belongs to', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
      );
      addTearDown(hub.dispose);

      final List<Payload> btc = <Payload>[];
      final List<Payload> eth = <Payload>[];
      hub.stream(ticker('BTC')).listen(btc.add);
      hub.stream(ticker('ETH')).listen(eth.add);
      await hub.whenReady();

      transport.emit('{"channel":"ticker","symbol":"BTC","data":{"last":1}}');
      await settle();

      expect(btc, <Payload>[
        <String, Object?>{'last': 1},
      ]);
      expect(eth, isEmpty);
    });

    test('a fanned-out channel reaches both its keys', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(fanOut: <String>{'orders'}),
      );
      addTearDown(hub.dispose);

      final List<Payload> perSymbol = <Payload>[];
      final List<Payload> everything = <Payload>[];
      hub
          .stream(SubscriptionKey('orders', <String, String>{'symbol': 'BTC'}))
          .listen(perSymbol.add);
      hub.stream(SubscriptionKey('orders')).listen(everything.add);
      await hub.whenReady();

      transport.emit('{"channel":"orders","symbol":"BTC","data":{"id":9}}');
      await settle();

      expect(perSymbol, hasLength(1));
      expect(everything, hasLength(1));
    });

    test('an unroutable frame is dropped and logged, not thrown', () async {
      final FakeTransport transport = FakeTransport();
      final List<String> logs = <String>[];
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
        log: (String message, {Object? error, StackTrace? stackTrace}) =>
            logs.add(message),
      );
      addTearDown(hub.dispose);

      await hub.connect();
      transport.emit('{"channel":"depth","symbol":"BTC","data":{}}');
      await settle();

      expect(logs.join('\n'), contains('depth'));
    });

    test('retainLatest hands the last payload to a late listener', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
        retainLatest: true,
      );
      addTearDown(hub.dispose);

      hub.stream(ticker('BTC')).listen((_) {});
      await hub.whenReady();
      transport.emit('{"channel":"ticker","symbol":"BTC","data":{"last":7}}');
      await settle();

      expect(hub.latest(ticker('BTC')), <String, Object?>{'last': 7});

      final List<Payload> late = <Payload>[];
      hub.stream(ticker('BTC')).listen(late.add);
      await settle();

      expect(late, <Payload>[
        <String, Object?>{'last': 7},
      ]);
    });

    test('nothing is cached for a key nobody asked for', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
        retainLatest: true,
      );
      addTearDown(hub.dispose);

      await hub.connect();
      transport.emit('{"channel":"ticker","symbol":"DOGE","data":{"last":1}}');
      await settle();

      expect(hub.latest(ticker('DOGE')), isNull);
    });
  });

  group('reconnecting', () {
    test('resubscribes everything on the new socket, streams intact', () async {
      final List<FakeTransport> transports = <FakeTransport>[];
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () {
          final FakeTransport transport = FakeTransport();
          transports.add(transport);
          return transport;
        },
        codec: codec(),
        reconnectPolicy: const ReconnectPolicy(
          initialDelay: Duration(milliseconds: 1),
          jitter: 0,
        ),
      );
      addTearDown(hub.dispose);

      final List<Payload> seen = <Payload>[];
      bool done = false;
      hub.stream(ticker('BTC')).listen(seen.add, onDone: () => done = true);
      hub.stream(trades('BTC')).listen((_) {});
      await hub.whenReady();
      await settle();
      expect(transports, hasLength(1));

      await transports.first.closeByPeer();
      await Future<void>.delayed(const Duration(milliseconds: 30));
      await settle();

      expect(transports, hasLength(2), reason: 'a fresh socket per attempt');
      expect(hub.connectionState, SocketConnectionState.ready);
      expect(
        argsOf(transports.last.sent, 'subscribe'),
        hasLength(2),
        reason: 'both subscriptions re-sent in one frame',
      );

      transports.last.emit(
        '{"channel":"ticker","symbol":"BTC","data":{"last":2}}',
      );
      await settle();

      expect(seen, hasLength(1), reason: 'the same stream keeps delivering');
      expect(done, isFalse, reason: 'a drop is a gap, not a done event');
    });

    test('a dying socket reporting twice still reconnects once', () async {
      final List<FakeTransport> transports = <FakeTransport>[];
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () {
          final FakeTransport transport = FakeTransport();
          transports.add(transport);
          return transport;
        },
        codec: codec(),
        reconnectPolicy: const ReconnectPolicy(
          initialDelay: Duration(milliseconds: 1),
          jitter: 0,
        ),
      );
      addTearDown(hub.dispose);

      hub.stream(ticker('BTC')).listen((_) {});
      await hub.whenReady();

      transports.first.emitError(StateError('boom'));
      await transports.first.closeByPeer();
      await Future<void>.delayed(const Duration(milliseconds: 30));
      await settle();

      expect(transports, hasLength(2));
    });

    test('giving up errors every stream, then closes the hub', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
        reconnectPolicy: const ReconnectPolicy.none(),
      );

      Object? error;
      bool done = false;
      hub
          .stream(ticker('BTC'))
          .listen(
            (_) {},
            onError: (Object e) => error = e,
            onDone: () => done = true,
          );
      await hub.whenReady();

      await transport.closeByPeer();
      await settle();

      expect(error, isA<SocketClosedException>());
      expect(done, isTrue);
      expect(hub.connectionState, SocketConnectionState.closed);
      expect(() => hub.stream(ticker('BTC')), throwsStateError);
    });

    test('a refused connection is retried, not thrown', () async {
      int attempts = 0;
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () {
          attempts++;
          final FakeTransport transport = FakeTransport(autoReady: false);
          if (attempts == 1) {
            transport.failReady(StateError('refused'));
          } else {
            transport.completeReady();
          }
          return transport;
        },
        codec: codec(),
        reconnectPolicy: const ReconnectPolicy(
          initialDelay: Duration(milliseconds: 1),
          jitter: 0,
        ),
      );
      addTearDown(hub.dispose);

      await hub.connect(); // does not throw
      expect(hub.connectionState, SocketConnectionState.reconnecting);

      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(attempts, 2);
      expect(hub.connectionState, SocketConnectionState.ready);
    });
  });

  group('handshake', () {
    test('logs in before any subscription goes out', () async {
      final List<Object> order = <Object>[];
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: _LoginCodec(transport),
      );
      addTearDown(hub.dispose);

      hub.stream(ticker('BTC')).listen((_) {});
      await hub.whenReady();
      await settle();

      for (final Object frame in transport.sent) {
        order.add((jsonDecode(frame as String) as Map<String, Object?>)['op']!);
      }
      expect(order, <String>['login', 'subscribe']);
    });

    test('a refused login fails the attempt and reconnects', () async {
      int attempts = 0;
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () {
          final int attempt = ++attempts;
          // The first socket refuses the login; the second accepts it.
          late final FakeTransport transport;
          transport = FakeTransport(
            onSend: (Object frame) => scheduleMicrotask(
              () => transport.emit(
                jsonEncode(<String, Object?>{
                  'op': 'login',
                  'error': attempt == 1 ? 'token expired' : null,
                }),
              ),
            ),
          );
          return transport;
        },
        codec: const _AlwaysLoginCodec(),
        reconnectPolicy: const ReconnectPolicy(
          initialDelay: Duration(milliseconds: 1),
          jitter: 0,
        ),
      );
      addTearDown(hub.dispose);

      await hub.connect();
      expect(hub.connectionState, SocketConnectionState.reconnecting);

      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(attempts, 2);
      expect(hub.connectionState, SocketConnectionState.ready);
    });

    test(
      'a handshake waiting for a reply that never comes times out',
      () async {
        final FakeTransport transport = FakeTransport();
        final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
          transport: () => transport,
          codec: const _AlwaysLoginCodec(timeout: Duration(milliseconds: 5)),
          reconnectPolicy: const ReconnectPolicy.none(),
        );

        await hub.connect();
        expect(hub.connectionState, SocketConnectionState.closed);
      },
    );
  });

  group('keepalive', () {
    test('sends the codec heartbeat on its interval', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(heartbeat: const <String, Object?>{'op': 'ping'}),
        heartbeatInterval: const Duration(milliseconds: 5),
      );
      addTearDown(hub.dispose);

      await hub.connect();
      await Future<void>.delayed(const Duration(milliseconds: 30));

      expect(
        transport.sent.where(
          (Object frame) => (frame as String).contains('"op":"ping"'),
        ),
        isNotEmpty,
      );
    });

    test('a socket that says nothing for too long is reconnected', () async {
      final List<FakeTransport> transports = <FakeTransport>[];
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () {
          final FakeTransport transport = FakeTransport();
          transports.add(transport);
          return transport;
        },
        codec: codec(),
        heartbeatInterval: const Duration(milliseconds: 5),
        idleTimeout: const Duration(milliseconds: 10),
        reconnectPolicy: const ReconnectPolicy(
          initialDelay: Duration(milliseconds: 1),
          jitter: 0,
        ),
      );
      addTearDown(hub.dispose);

      await hub.connect();
      await Future<void>.delayed(const Duration(milliseconds: 60));

      expect(transports.length, greaterThan(1));
    });
  });

  group('lifecycle', () {
    test('disconnect keeps subscriptions; connect restores them', () async {
      final List<FakeTransport> transports = <FakeTransport>[];
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () {
          final FakeTransport transport = FakeTransport();
          transports.add(transport);
          return transport;
        },
        codec: codec(),
      );
      addTearDown(hub.dispose);

      bool done = false;
      hub.stream(ticker('BTC')).listen((_) {}, onDone: () => done = true);
      await hub.whenReady();
      await settle();

      await hub.disconnect();
      expect(hub.connectionState, SocketConnectionState.idle);
      expect(hub.wireSubscriptions, isEmpty);
      expect(hub.activeSubscriptions, hasLength(1));
      expect(done, isFalse);

      await hub.connect();
      await settle();

      expect(transports, hasLength(2));
      expect(argsOf(transports.last.sent, 'subscribe'), hasLength(1));
    });

    test('states are reported in order', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
      );

      final List<SocketConnectionState> states = <SocketConnectionState>[];
      hub.connectionStates.listen(states.add);

      await hub.connect();
      await hub.dispose();
      await settle();

      expect(states, <SocketConnectionState>[
        SocketConnectionState.connecting,
        SocketConnectionState.ready,
        SocketConnectionState.closed,
      ]);
    });

    test('dispose closes the streams and bars further use', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
      );

      bool done = false;
      hub.stream(ticker('BTC')).listen((_) {}, onDone: () => done = true);
      await hub.whenReady();

      await hub.dispose();
      await hub.dispose(); // idempotent
      await settle();

      expect(done, isTrue);
      expect(transport.isClosed, isTrue);
      expect(() => hub.stream(ticker('BTC')), throwsStateError);
      expect(() => hub.send('x'), throwsStateError);
      expect(hub.connect, throwsStateError);
    });

    test('send is a no-op with no socket, and reaches one when open', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
        autoConnect: false,
      );
      addTearDown(hub.dispose);

      expect(hub.send('{"op":"noop"}'), isFalse);

      await hub.connect();
      expect(hub.send('{"op":"noop"}'), isTrue);
      expect(transport.sent, contains('{"op":"noop"}'));
    });

    test('autoConnect false leaves the socket shut until asked', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
        autoConnect: false,
      );
      addTearDown(hub.dispose);

      hub.stream(ticker('BTC')).listen((_) {});
      await settle();

      expect(hub.connectionState, SocketConnectionState.idle);
      expect(transport.sent, isEmpty);

      await hub.connect();
      await settle();
      expect(argsOf(transport.sent, 'subscribe'), hasLength(1));
    });

    test('control frames are published for a host to watch', () async {
      final FakeTransport transport = FakeTransport();
      final SocketChannelHub<Payload> hub = SocketChannelHub<Payload>(
        transport: () => transport,
        codec: codec(),
      );
      addTearDown(hub.dispose);

      final List<SocketControl<Object?>> controls = <SocketControl<Object?>>[];
      hub.controlFrames.listen(controls.add);
      await hub.connect();

      transport.emit('{"op":"subscribe","error":"unknown channel"}');
      await settle();

      expect(controls.single.op, 'subscribe');
      expect(controls.single.error, 'unknown channel');
    });
  });
}

/// A codec that logs in with a hard-coded token and waits for the reply, with
/// the fake answering it.
class _LoginCodec extends JsonSocketCodec<Payload> {
  _LoginCodec(this._transport)
    : super(parsers: <String, JsonPayloadParser<Payload>>{'ticker': parse});

  final FakeTransport _transport;

  @override
  Future<void> handshake(SocketHandshake socket) async {
    socket.send(jsonEncode(<String, Object?>{'op': 'login', 'token': 't'}));
    scheduleMicrotask(() => _transport.emit('{"op":"login","error":null}'));
    await socket.expect('login');
  }
}

/// A codec that always logs in, leaving the reply to the transport.
class _AlwaysLoginCodec extends SocketCodec<Payload> {
  const _AlwaysLoginCodec({this.timeout = const Duration(seconds: 10)});

  final Duration timeout;

  @override
  Object? encodeSubscribe(List<SubscriptionKey> keys) => null;

  @override
  Object? encodeUnsubscribe(List<SubscriptionKey> keys) => null;

  @override
  SocketDecoded<Payload> decode(Object? frame) {
    final Object? json = frame is String ? jsonDecode(frame) : frame;
    if (json is Map && json['op'] is String) {
      return SocketControl<Payload>(
        json['op']! as String,
        error: json['error'],
      );
    }
    return const SocketIgnored<Payload>('not control');
  }

  @override
  Future<void> handshake(SocketHandshake socket) async {
    socket.send(jsonEncode(<String, Object?>{'op': 'login', 'token': 't'}));
    await socket.expect('login', timeout: timeout);
  }
}
