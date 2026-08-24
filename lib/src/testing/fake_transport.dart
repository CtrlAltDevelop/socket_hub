import 'dart:async';

import '../transport.dart';

/// A [SocketTransport] with no socket behind it, for tests and mock modes.
///
/// Everything the far end would do is a method call: [emit] delivers a frame,
/// [emitError] reports a socket error, [closeByPeer] hangs up. Everything the
/// hub sends is recorded in [sent].
///
/// ```dart
/// final transport = FakeTransport();
/// final hub = SocketChannelHub<Object?>(
///   transport: () => transport,
///   codec: codec,
/// );
///
/// hub.stream(SubscriptionKey('ticker', {'symbol': 'BTC'})).listen(events.add);
/// await hub.whenReady();
/// expect(transport.sent.single, contains('"channel":"ticker"'));
///
/// transport.emit('{"channel":"ticker","symbol":"BTC","data":{"last":1}}');
/// ```
///
/// A hub that reconnects calls its transport factory again, so return a new
/// `FakeTransport` per call — or the same one, to assert it is not reused.
class FakeTransport implements SocketTransport {
  /// Creates a fake transport.
  ///
  /// With [autoReady] false, [ready] stays pending until [completeReady] or
  /// [failReady] is called, which is how a slow or refused connection is
  /// tested. [onSend] sees every frame as it is sent, which is how
  /// `MockSocketServer` answers them.
  FakeTransport({bool autoReady = true, this.onSend}) {
    if (autoReady) _ready.complete();
  }

  final Completer<void> _ready = Completer<void>();
  final StreamController<dynamic> _incoming = StreamController<dynamic>();

  /// Called with each frame the hub sends, before it is added to [sent].
  final void Function(Object frame)? onSend;

  /// Every frame the hub has sent, oldest first.
  final List<Object> sent = <Object>[];

  bool _closed = false;

  /// Whether [close] has been called, or the peer hung up.
  bool get isClosed => _closed;

  /// The frames sent since the last call to this getter.
  ///
  /// Handy between assertions: each step of a test sees only its own traffic.
  List<Object> takeSent() {
    final List<Object> taken = List<Object>.of(sent);
    sent.clear();
    return taken;
  }

  @override
  Stream<dynamic> get incoming => _incoming.stream;

  @override
  Future<void> get ready => _ready.future;

  @override
  void send(Object data) {
    if (_closed) throw StateError('Sent on a closed FakeTransport.');
    onSend?.call(data);
    sent.add(data);
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    // Not awaited on purpose: a single-subscription controller that was never
    // listened to never completes its close future, and a transport whose
    // `ready` failed is exactly that case.
    if (!_incoming.isClosed) unawaited(_incoming.close());
  }

  /// Lets a pending [ready] succeed. Only for `autoReady: false`.
  void completeReady() {
    if (!_ready.isCompleted) _ready.complete();
  }

  /// Fails a pending [ready] with [error], as a refused connection would.
  void failReady(Object error) {
    if (!_ready.isCompleted) _ready.completeError(error);
  }

  /// Delivers [frame] to the hub, as the far end would.
  void emit(Object? frame) {
    if (_incoming.isClosed) return;
    _incoming.add(frame);
  }

  /// Reports [error] on the socket, as a transport-level failure would.
  void emitError(Object error, [StackTrace? stackTrace]) {
    if (_incoming.isClosed) return;
    _incoming.addError(error, stackTrace ?? StackTrace.current);
  }

  /// Closes the socket from the far end, without an error.
  ///
  /// Leaves the transport closed, so a frame sent afterwards throws the way a
  /// real hung-up socket would.
  Future<void> closeByPeer() async {
    if (_incoming.isClosed) return;
    _closed = true;
    unawaited(_incoming.close());
  }
}
