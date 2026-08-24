import 'dart:async';
import 'dart:convert';

import '../subscription_key.dart';
import 'fake_transport.dart';

/// Builds the payload for one subscription on one tick.
///
/// Returning null skips that subscription for that tick, which is how a feed
/// stays quiet on channels it has nothing to say about.
typedef MockPayloadBuilder = Object? Function(SubscriptionKey key, int tick);

/// A stand-in server: it reads the subscribe frames a hub sends and pushes
/// generated payloads back for whatever is subscribed.
///
/// This is mock mode without a parallel code path — the hub, the codec and the
/// app's stream wiring are the real ones, and only the far end is invented. It
/// also makes a good test double for anything that consumes a hub.
///
/// ```dart
/// final server = MockSocketServer(
///   build: (key, tick) => switch (key.channel) {
///     'ticker' => {'symbol': key.args['symbol'], 'last': 64000 + tick},
///     'candle' => {'close': 64000 + tick, 'interval': key.args['interval']},
///     _ => null,
///   },
/// );
///
/// final hub = SocketChannelHub<Object?>(
///   transport: server.open,
///   codec: codec,
/// );
/// ```
///
/// The frames it emits match `JsonSocketCodec`'s inbound shape, and its field
/// names are configurable for a codec set up differently.
class MockSocketServer {
  /// Creates a mock server that answers with [build] every [tick].
  ///
  /// With [ackOps] on, every `{"op": …}` frame is answered with a matching
  /// success control frame, so a codec whose handshake waits for a login reply
  /// gets one.
  MockSocketServer({
    required this.build,
    this.tick = const Duration(seconds: 1),
    this.ackOps = true,
    this.opField = 'op',
    this.channelField = 'channel',
    this.dataField = 'data',
    this.argsField = 'args',
    this.errorField = 'error',
    this.subscribeOp = 'subscribe',
    this.unsubscribeOp = 'unsubscribe',
  });

  /// Builds the payload for one subscription on one tick.
  final MockPayloadBuilder build;

  /// How often a round of payloads goes out.
  final Duration tick;

  /// Whether control frames are answered with a success reply.
  final bool ackOps;

  /// The field naming an operation, on frames in and out.
  final String opField;

  /// The field naming a channel on an outbound data frame.
  final String channelField;

  /// The field carrying an outbound payload.
  final String dataField;

  /// The field holding the argument list of an inbound control frame.
  final String argsField;

  /// The field an acknowledgement reports its (absent) error in.
  final String errorField;

  /// The inbound `op` value that subscribes.
  final String subscribeOp;

  /// The inbound `op` value that unsubscribes.
  final String unsubscribeOp;

  final Set<SubscriptionKey> _subscribed = <SubscriptionKey>{};
  FakeTransport? _transport;
  Timer? _timer;
  int _tick = 0;

  /// What the hub has subscribed to, as read off the wire.
  Set<SubscriptionKey> get subscriptions =>
      Set<SubscriptionKey>.unmodifiable(_subscribed);

  /// How many rounds have gone out.
  int get tickCount => _tick;

  /// The transport currently connected, if any. Exposed so a test can drop the
  /// socket with `transport!.closeByPeer()` and watch the hub come back.
  FakeTransport? get transport => _transport;

  /// Opens a connection. Pass this straight to a hub as its transport factory.
  ///
  /// Each call supersedes the last, and clears the subscriptions the previous
  /// socket had — as a real server would, since a new socket knows nothing
  /// about an old one's state.
  FakeTransport open() {
    _timer?.cancel();
    _subscribed.clear();
    final FakeTransport transport = FakeTransport(onSend: _onFrame);
    _transport = transport;
    _timer = Timer.periodic(tick, (_) => pump());
    return transport;
  }

  /// Sends one round of payloads now, without waiting for the timer.
  ///
  /// Tests should drive the feed with this and leave [tick] long, rather than
  /// sleeping.
  void pump() {
    final FakeTransport? transport = _transport;
    if (transport == null || transport.isClosed) return;
    final int tick = _tick++;
    for (final SubscriptionKey key in _subscribed.toList(growable: false)) {
      final Object? payload = build(key, tick);
      if (payload == null) continue;
      transport.emit(
        jsonEncode(<String, Object?>{
          channelField: key.channel,
          ...key.args,
          dataField: payload,
        }),
      );
    }
  }

  /// Stops the feed and closes the current socket.
  Future<void> dispose() async {
    _timer?.cancel();
    _timer = null;
    final FakeTransport? transport = _transport;
    _transport = null;
    _subscribed.clear();
    await transport?.close();
  }

  void _onFrame(Object frame) {
    final Map<String, Object?>? json = _decode(frame);
    if (json == null) return;
    final Object? op = json[opField];
    if (op is! String) return;

    if (op == subscribeOp || op == unsubscribeOp) {
      final Object? args = json[argsField];
      if (args is List) {
        for (final Object? entry in args) {
          final SubscriptionKey? key = _keyFrom(entry);
          if (key == null) continue;
          if (op == subscribeOp) {
            _subscribed.add(key);
          } else {
            _subscribed.remove(key);
          }
        }
      }
    }

    if (!ackOps) return;
    // Scheduled rather than sent inline: a real reply never arrives inside the
    // send that caused it, and code under test should not depend on it doing.
    scheduleMicrotask(() {
      _transport?.emit(
        jsonEncode(<String, Object?>{opField: op, errorField: null}),
      );
    });
  }

  SubscriptionKey? _keyFrom(Object? entry) {
    if (entry is! Map) return null;
    final Map<String, Object?> map = entry.cast<String, Object?>();
    final Object? channel = map[channelField];
    if (channel is! String) return null;
    return SubscriptionKey(channel, <String, String?>{
      for (final MapEntry<String, Object?> e in map.entries)
        if (e.key != channelField) e.key: e.value?.toString(),
    });
  }

  Map<String, Object?>? _decode(Object frame) {
    if (frame is Map) return frame.cast<String, Object?>();
    if (frame is String) {
      try {
        final Object? decoded = jsonDecode(frame);
        return decoded is Map ? decoded.cast<String, Object?>() : null;
      } on FormatException {
        return null;
      }
    }
    return null;
  }
}
