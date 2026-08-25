import 'dart:async';
import 'dart:math';

import 'connection_state.dart';
import 'reconnect_policy.dart';
import 'socket_codec.dart';
import 'subscription_key.dart';
import 'transport.dart';

/// Where a hub sends what it wants to say about itself.
///
/// Wire it to whatever the app already logs through:
///
/// ```dart
/// log: (message, {error, stackTrace}) =>
///     logger.d(message, error: error, stackTrace: stackTrace),
/// ```
typedef SocketLogger = void Function(
  String message, {
  Object? error,
  StackTrace? stackTrace,
});

/// A live subscription held open without a listener.
///
/// Most code should just listen to [SocketChannelHub.stream], which counts
/// listeners on its own. A lease is for the case where the data has to keep
/// flowing while nothing is watching — priming a cache before a screen opens,
/// or holding a symbol subscribed across a page transition.
///
/// Always [close] it. A lease that is never closed keeps its subscription on
/// the wire for the life of the hub.
class SubscriptionLease {
  SubscriptionLease._(this._release, this.keys);

  final void Function() _release;
  bool _closed = false;

  /// The subscriptions this lease holds.
  final List<SubscriptionKey> keys;

  /// Whether [close] has been called.
  bool get isClosed => _closed;

  /// Releases the subscriptions. Safe to call more than once.
  void close() {
    if (_closed) return;
    _closed = true;
    _release();
  }
}

/// One socket, many channels.
///
/// The hub owns a single connection and hands out a stream per subscription.
/// It counts how many callers want each subscription, sends one wire frame
/// when the count reaches one and another when it falls to zero, batches
/// everything asked for in the same turn of the event loop into a single
/// frame, and re-sends every live subscription after a reconnect.
///
/// What the server's frames actually look like is a [SocketCodec]'s business,
/// not the hub's.
///
/// ```dart
/// final hub = SocketChannelHub<Object?>(
///   transport: () => WebSocketTransport.connect(url),
///   codec: myCodec,
/// );
///
/// // Subscribes on listen, unsubscribes on cancel. One frame, not two.
/// final sub = hub.stream(SubscriptionKey('ticker', {'symbol': 'BTCUSDT'}))
///     .listen(onTicker);
/// hub.stream(SubscriptionKey('trades', {'symbol': 'BTCUSDT'})).listen(onTrade);
///
/// await sub.cancel();      // sends the unsubscribe
/// await hub.dispose();
/// ```
class SocketChannelHub<T> {
  /// Creates a hub over the socket [transport] opens, speaking [codec].
  ///
  /// [transport] is called once per connection attempt, so every reconnect
  /// gets a fresh socket. Nothing is opened until the first subscription
  /// arrives, unless [autoConnect] is false — in which case [connect] has to
  /// be called explicitly.
  ///
  /// With [retainLatest] on, the most recent payload per subscription is kept
  /// and handed to each new listener immediately, so a widget built after a
  /// tick has already gone past does not sit empty until the next one.
  ///
  /// [heartbeatInterval] sends [SocketCodec.encodeHeartbeat] on a timer, and
  /// [idleTimeout] treats a socket that has delivered nothing for that long
  /// as dead and reconnects it. Both are off when null.
  SocketChannelHub({
    required SocketTransportFactory transport,
    required this.codec,
    this.reconnectPolicy = const ReconnectPolicy(),
    this.retainLatest = false,
    this.autoConnect = true,
    this.heartbeatInterval,
    this.idleTimeout,
    this.log,
    this.random,
  }) : _transportFactory = transport;

  final SocketTransportFactory _transportFactory;

  /// The protocol this hub speaks.
  final SocketCodec<T> codec;

  /// Where the hub reports what it is doing, or null to say nothing.
  final SocketLogger? log;

  /// The source of reconnect jitter. Pass a seeded [Random] to make a test's
  /// backoff reproducible.
  final Random? random;

  /// How long to wait before each reconnect attempt, and when to stop.
  final ReconnectPolicy reconnectPolicy;

  /// Whether each subscription's most recent payload is replayed to new
  /// listeners.
  final bool retainLatest;

  /// Whether the first subscription opens the socket on its own.
  final bool autoConnect;

  /// How often to send the codec's heartbeat frame, or null for never.
  final Duration? heartbeatInterval;

  /// How long a silent socket may stay open before it is treated as dead.
  final Duration? idleTimeout;

  final Map<SubscriptionKey, StreamController<T>> _controllers =
      <SubscriptionKey, StreamController<T>>{};
  final Map<SubscriptionKey, int> _refCounts = <SubscriptionKey, int>{};
  final Set<SubscriptionKey> _onWire = <SubscriptionKey>{};
  final Map<SubscriptionKey, T> _latest = <SubscriptionKey, T>{};

  final StreamController<SocketConnectionState> _states =
      StreamController<SocketConnectionState>.broadcast();
  final StreamController<SocketControl<Object?>> _controls =
      StreamController<SocketControl<Object?>>.broadcast();

  SocketTransport? _transport;
  StreamSubscription<dynamic>? _socketSub;
  Future<void>? _opening;
  Timer? _reconnectTimer;
  Timer? _heartbeatTimer;
  DateTime? _lastFrameAt;
  List<SocketControl<Object?>>? _handshakeBuffer;
  int _attempt = 0;
  bool _flushScheduled = false;
  bool _failing = false;
  bool _disposed = false;

  SocketConnectionState _state = SocketConnectionState.idle;

  /// The hub's current place in its connection lifecycle.
  SocketConnectionState get connectionState => _state;

  /// Every change to [connectionState]. Broadcast; does not replay.
  Stream<SocketConnectionState> get connectionStates => _states.stream;

  /// Every control frame the codec reported, whether it named an error or not.
  ///
  /// Watch it to surface protocol trouble a subscription stream cannot show —
  /// a rejected subscribe, an expired session. Broadcast; does not replay.
  Stream<SocketControl<Object?>> get controlFrames => _controls.stream;

  /// The subscriptions at least one caller currently wants.
  Set<SubscriptionKey> get activeSubscriptions =>
      Set<SubscriptionKey>.unmodifiable(_refCounts.keys);

  /// The subscriptions the server has actually been told about.
  ///
  /// Equal to [activeSubscriptions] while the socket is ready, and empty while
  /// it is not. Exposed for tests and diagnostics.
  Set<SubscriptionKey> get wireSubscriptions =>
      Set<SubscriptionKey>.unmodifiable(_onWire);

  /// How many callers hold [key] — every listener, plus every lease.
  int refCount(SubscriptionKey key) => _refCounts[key] ?? 0;

  /// The payload last routed to [key], when [retainLatest] is on.
  T? latest(SubscriptionKey key) => _latest[key];

  // ---------------------------------------------------------------- streams

  /// The payloads routed to [key].
  ///
  /// Listening subscribes on the wire if nobody else had; cancelling the last
  /// listener unsubscribes. Both are batched, so listening to four streams in
  /// one turn of the event loop sends one frame.
  ///
  /// The stream survives reconnects: a drop shows up as a gap in events, not
  /// as a done event, and the subscription is re-sent when the socket returns.
  /// It closes only when the hub is disposed, or when the reconnect policy
  /// gives up — and in that case the last connection error arrives first.
  ///
  /// Safe to call before [connect]: the subscription is remembered and sent as
  /// soon as a socket is ready.
  Stream<T> stream(SubscriptionKey key) {
    _assertUsable();
    final StreamController<T> controller = _controllerFor(key);

    // Stream.multi rather than the controller's own stream, so each listener
    // is counted: a broadcast controller's onListen fires only on the first
    // one, which would make two listeners look like one caller.
    return Stream<T>.multi((MultiStreamController<T> out) {
      _retain(key);
      // Subscribing first and replaying second cannot reorder anything: both
      // happen inside this one synchronous block, and a controller delivers
      // its queue in the order things were added to it.
      final StreamSubscription<T> sub = controller.stream.listen(
        out.add,
        onError: out.addError,
        onDone: out.close,
      );
      if (retainLatest) {
        final T? cached = _latest[key];
        if (cached != null) out.add(cached);
      }
      // Released before the inner cancel is awaited, not after: awaiting first
      // would push the release into a later microtask than the pending flush,
      // and a listener opened and closed in one turn would send both frames
      // instead of neither.
      out.onCancel = () {
        _release(key);
        return sub.cancel();
      };
    });
  }

  /// Holds [key] subscribed with no listener attached.
  ///
  /// The returned lease must be closed. See [SubscriptionLease].
  SubscriptionLease subscribe(SubscriptionKey key) =>
      subscribeAll(<SubscriptionKey>[key]);

  /// Holds every key in [keys] subscribed, as one lease and one wire frame.
  SubscriptionLease subscribeAll(Iterable<SubscriptionKey> keys) {
    _assertUsable();
    final List<SubscriptionKey> held = keys.toList(growable: false);
    for (final SubscriptionKey key in held) {
      _retain(key);
    }
    return SubscriptionLease._(() {
      for (final SubscriptionKey key in held) {
        _release(key);
      }
    }, held);
  }

  StreamController<T> _controllerFor(SubscriptionKey key) => _controllers
      .putIfAbsent(key, () => StreamController<T>.broadcast(sync: true));

  void _retain(SubscriptionKey key) {
    final int next = (_refCounts[key] ?? 0) + 1;
    _refCounts[key] = next;
    if (next != 1) return;
    _scheduleFlush();
    if (autoConnect) unawaited(_ensureConnected());
  }

  void _release(SubscriptionKey key) {
    final int current = _refCounts[key] ?? 0;
    if (current <= 1) {
      _refCounts.remove(key);
      _latest.remove(key);
      _scheduleFlush();
    } else {
      _refCounts[key] = current - 1;
    }
  }

  void _scheduleFlush() {
    if (_flushScheduled || _disposed) return;
    _flushScheduled = true;
    scheduleMicrotask(() {
      _flushScheduled = false;
      _reconcile();
    });
  }

  /// Brings the wire in line with what callers want.
  ///
  /// Works from the difference between the two sets rather than from a queue
  /// of deltas, so a subscribe and an unsubscribe of the same key in one turn
  /// cancel out, and a reconnect is this same call with an empty wire set.
  void _reconcile() {
    if (_disposed || !_state.isReady) return;

    final List<SubscriptionKey> toSubscribe = <SubscriptionKey>[
      for (final SubscriptionKey key in _refCounts.keys)
        if (!_onWire.contains(key)) key,
    ];
    final List<SubscriptionKey> toUnsubscribe = <SubscriptionKey>[
      for (final SubscriptionKey key in _onWire)
        if (!_refCounts.containsKey(key)) key,
    ];

    if (toSubscribe.isNotEmpty) {
      final Object? frame = codec.encodeSubscribe(toSubscribe);
      if (frame == null || _sendRaw(frame)) _onWire.addAll(toSubscribe);
    }
    if (toUnsubscribe.isNotEmpty) {
      final Object? frame = codec.encodeUnsubscribe(toUnsubscribe);
      if (frame != null) _sendRaw(frame);
      // Dropped from the wire set either way: if the frame could not be sent
      // the socket is gone, and the next one starts with nothing subscribed.
      _onWire.removeAll(toUnsubscribe);
    }
  }

  // ------------------------------------------------------------- lifecycle

  /// Opens the socket, if it is not open already.
  ///
  /// Returns once the socket is ready and the handshake has run, or once the
  /// attempt has failed and a reconnect has been scheduled — it does not throw
  /// on a failed connection, because the reconnect policy decides what happens
  /// next. Concurrent calls share one attempt.
  Future<void> connect() {
    _assertUsable();
    return _ensureConnected();
  }

  Future<void> _ensureConnected() {
    if (_disposed || _state.isReady) return Future<void>.value();
    return _opening ??= _open().whenComplete(() => _opening = null);
  }

  /// Completes the next time the hub reaches [SocketConnectionState.ready].
  ///
  /// Returns immediately if it is ready now. Throws a [StateError] if the hub
  /// is closed, or closes while this is waiting.
  Future<void> whenReady() async {
    if (_state.isReady) return;
    if (_state.isClosed) {
      throw StateError('This SocketChannelHub is closed.');
    }
    await for (final SocketConnectionState state in _states.stream) {
      if (state.isReady) return;
      if (state.isClosed) break;
    }
    throw StateError('The hub closed before it became ready.');
  }

  /// Closes the socket and stops reconnecting, keeping every subscription.
  ///
  /// Streams stay open and reference counts are kept, so a later [connect]
  /// restores exactly what was subscribed before. Listeners see a gap, not a
  /// done event — which is the point: a host can drop the socket while the app
  /// is backgrounded and pick it up again on resume without rebuilding
  /// anything.
  Future<void> disconnect() async {
    _cancelReconnect();
    _attempt = 0;
    // Cleared with the socket. It guards against one dying connection
    // reporting itself twice; left set, it would swallow the failure of the
    // *next* attempt and leave the hub connecting with nothing scheduled.
    _failing = false;
    await _teardownSocket();
    if (!_disposed) _setState(SocketConnectionState.idle);
  }

  /// Closes the socket and every stream, for good.
  ///
  /// A disposed hub cannot be reused. Listeners get a done event.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    _cancelReconnect();
    await _teardownSocket();
    _setState(SocketConnectionState.closed);
    await _closeStreams();
  }

  Future<void> _closeStreams() async {
    final List<StreamController<T>> controllers = _controllers.values.toList();
    _controllers.clear();
    _refCounts.clear();
    _latest.clear();
    for (final StreamController<T> controller in controllers) {
      await controller.close();
    }
    await _states.close();
    await _controls.close();
  }

  Future<void> _open() async {
    _cancelReconnect();
    _setState(
      _attempt == 0
          ? SocketConnectionState.connecting
          : SocketConnectionState.reconnecting,
    );

    final SocketTransport transport = _transportFactory();
    _transport = transport;
    _onWire.clear();

    try {
      await transport.ready;
      if (_isStale(transport)) return;

      _socketSub = transport.incoming.listen(
        _onFrame,
        onError: _onSocketError,
        onDone: _onSocketDone,
      );

      _handshakeBuffer = <SocketControl<Object?>>[];
      try {
        await codec.handshake(_Handshake<T>(this, transport));
      } finally {
        _handshakeBuffer = null;
      }
      if (_isStale(transport)) return;
    } on Object catch (error, stackTrace) {
      if (_isStale(transport)) return;
      await _failConnection(error, stackTrace);
      return;
    }

    _attempt = 0;
    _failing = false;
    _lastFrameAt = DateTime.now();
    _setState(SocketConnectionState.ready);
    _startHeartbeat();
    _reconcile();
  }

  /// Whether [transport] has been superseded — the hub moved on while an await
  /// inside [_open] was in flight, so this attempt's outcome is void.
  bool _isStale(SocketTransport transport) =>
      _disposed || !identical(_transport, transport);

  Future<void> _failConnection(Object error, StackTrace stackTrace) async {
    // A dying socket reports itself more than once — an error event, then a
    // done event, then a failed send. The first one owns the reconnect.
    if (_failing || _disposed) return;
    _failing = true;
    _report('Connection failed', error: error, stackTrace: stackTrace);
    await _teardownSocket();
    if (_disposed) return;

    _attempt++;
    final Duration? delay = reconnectPolicy.delayFor(_attempt, random: random);
    if (delay == null) {
      _report(
        'Giving up after $_attempt attempt(s); closing every subscription.',
        error: error,
      );
      _disposed = true;
      _setState(SocketConnectionState.closed);
      for (final StreamController<T> controller in _controllers.values) {
        if (controller.hasListener) controller.addError(error, stackTrace);
      }
      await _closeStreams();
      return;
    }

    _setState(SocketConnectionState.reconnecting);
    _reconnectTimer = Timer(delay, () {
      _reconnectTimer = null;
      _failing = false;
      if (!_disposed) unawaited(_ensureConnected());
    });
  }

  Future<void> _teardownSocket() async {
    _heartbeatTimer?.cancel();
    _heartbeatTimer = null;
    final StreamSubscription<dynamic>? sub = _socketSub;
    _socketSub = null;
    await sub?.cancel();
    final SocketTransport? transport = _transport;
    _transport = null;
    _onWire.clear();
    await transport?.close();
  }

  void _cancelReconnect() {
    _reconnectTimer?.cancel();
    _reconnectTimer = null;
  }

  void _setState(SocketConnectionState state) {
    if (_state == state) return;
    _state = state;
    if (!_states.isClosed) _states.add(state);
  }

  // -------------------------------------------------------------- heartbeat

  void _startHeartbeat() {
    final Duration? interval = heartbeatInterval;
    if (interval == null) return;
    if (codec.encodeHeartbeat() == null && idleTimeout == null) return;

    _heartbeatTimer = Timer.periodic(interval, (_) {
      if (!_state.isReady) return;

      final Duration? limit = idleTimeout;
      final DateTime? last = _lastFrameAt;
      if (limit != null &&
          last != null &&
          DateTime.now().difference(last) > limit) {
        _report('Nothing received for $limit; treating the socket as dead.');
        unawaited(
          _failConnection(
            TimeoutException('Socket idle for longer than $limit'),
            StackTrace.current,
          ),
        );
        return;
      }

      final Object? frame = codec.encodeHeartbeat();
      if (frame != null) _sendRaw(frame);
    });
  }

  // ------------------------------------------------------------------ send

  /// Sends [frame] on the socket as it is, bypassing the codec.
  ///
  /// The escape hatch for a one-off the protocol needs and this package does
  /// not model. Returns false if there is no open socket to send it on.
  bool send(Object frame) {
    _assertUsable();
    return _sendRaw(frame);
  }

  bool _sendRaw(Object frame) {
    final SocketTransport? transport = _transport;
    if (transport == null) return false;
    try {
      transport.send(frame);
      return true;
    } on Object catch (error, stackTrace) {
      _report('Send failed', error: error, stackTrace: stackTrace);
      unawaited(_failConnection(error, stackTrace));
      return false;
    }
  }

  // --------------------------------------------------------------- inbound

  void _onFrame(dynamic raw) {
    _lastFrameAt = DateTime.now();

    final SocketDecoded<T> decoded;
    try {
      decoded = codec.decode(raw);
    } on Object catch (error, stackTrace) {
      _report(
        'Codec threw while decoding a frame',
        error: error,
        stackTrace: stackTrace,
      );
      return;
    }

    switch (decoded) {
      case SocketPayload<T>():
        _route(decoded);
      case SocketControl<T>():
        _onControl(
          SocketControl<Object?>(
            decoded.op,
            error: decoded.error,
            frame: decoded.frame,
          ),
        );
      case SocketIgnored<T>():
        _report('Dropped a frame: ${decoded.reason}');
    }
  }

  void _route(SocketPayload<T> payload) {
    for (final SubscriptionKey key in payload.keys) {
      final StreamController<T>? controller = _controllers[key];
      // Nothing is cached for a key nobody has asked for, so a server that
      // pushes hundreds of symbols cannot grow the cache without bound.
      if (retainLatest && _refCounts.containsKey(key)) {
        _latest[key] = payload.value;
      }
      if (controller != null && !controller.isClosed) {
        controller.add(payload.value);
      }
    }
  }

  void _onControl(SocketControl<Object?> control) {
    if (control.isError) {
      _report('Server reported a "${control.op}" error: ${control.error}');
    }
    _handshakeBuffer?.add(control);
    if (!_controls.isClosed) _controls.add(control);
  }

  void _onSocketError(Object error, StackTrace stackTrace) {
    _report('Socket error', error: error, stackTrace: stackTrace);
    unawaited(_failConnection(error, stackTrace));
  }

  void _onSocketDone() {
    if (_disposed || _transport == null) return;
    _report('The far end closed the socket.');
    unawaited(
      _failConnection(const SocketClosedException(), StackTrace.current),
    );
  }

  void _report(String message, {Object? error, StackTrace? stackTrace}) =>
      log?.call(message, error: error, stackTrace: stackTrace);

  void _assertUsable() {
    if (_disposed) {
      throw StateError('This SocketChannelHub is closed and cannot be reused.');
    }
  }
}

/// The error a hub reports when the far end closed the socket without one.
class SocketClosedException implements Exception {
  /// Creates the exception.
  const SocketClosedException();

  @override
  String toString() => 'SocketClosedException: the socket was closed remotely';
}

class _Handshake<T> implements SocketHandshake {
  _Handshake(this._hub, this._transport);

  final SocketChannelHub<T> _hub;
  final SocketTransport _transport;

  @override
  void send(Object frame) {
    if (!identical(_hub._transport, _transport)) {
      throw SocketHandshakeException('The socket went away mid-handshake.');
    }
    _transport.send(frame);
  }

  @override
  Stream<SocketControl<Object?>> get controlFrames => _hub.controlFrames;

  @override
  Future<SocketControl<Object?>> expect(
    String op, {
    Duration timeout = const Duration(seconds: 10),
  }) async {
    // A reply that landed before this call was buffered, so a handshake which
    // awaits something else in between does not miss its own answer.
    final List<SocketControl<Object?>> buffered =
        _hub._handshakeBuffer ?? const <SocketControl<Object?>>[];
    for (final SocketControl<Object?> control in buffered) {
      if (control.op == op) return _checked(op, control);
    }

    final SocketControl<Object?> control;
    try {
      control = await controlFrames
          .firstWhere((SocketControl<Object?> frame) => frame.op == op)
          .timeout(timeout);
    } on TimeoutException catch (error) {
      throw SocketHandshakeException(
        'No "$op" reply within $timeout',
        cause: error,
      );
    } on StateError catch (error) {
      throw SocketHandshakeException(
        'The connection ended before a "$op" reply arrived',
        cause: error,
      );
    }
    return _checked(op, control);
  }

  SocketControl<Object?> _checked(String op, SocketControl<Object?> control) {
    if (control.isError) {
      throw SocketHandshakeException(
        'The server refused "$op": ${control.error}',
        cause: control.error,
      );
    }
    return control;
  }
}
