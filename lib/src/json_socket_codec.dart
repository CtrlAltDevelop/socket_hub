import 'dart:convert';

import 'socket_codec.dart';
import 'subscription_key.dart';

/// Turns one channel's `data` field into the host's payload type.
///
/// Throwing is fine: the codec catches it, and the frame is reported as
/// ignored rather than taking the connection down.
typedef JsonPayloadParser<T> = T Function(Object? data);

/// Reads the error a control frame reports, or null when it reports success.
///
/// The default reads [JsonSocketCodec.errorField]. Supply one for a server
/// that spells failure some other way:
///
/// ```dart
/// errorReader: (frame) => frame['success'] == false ? frame['msg'] : null,
/// ```
typedef JsonErrorReader = Object? Function(Map<String, Object?> frame);

/// A codec for the `{"op": "subscribe", "args": [...]}` JSON convention that
/// most exchange and market-data sockets speak.
///
/// It handles the shape below in both directions, so a host that speaks it
/// needs no codec of its own — only a parser per channel.
///
/// Outbound:
///
/// ```json
/// {"op":"subscribe","args":[{"channel":"ticker","symbol":"BTCUSDT"}]}
/// ```
///
/// Inbound data, with the key's fields read from the top level, from `args`,
/// or from inside `data` — whichever carries them:
///
/// ```json
/// {"channel":"ticker","symbol":"BTCUSDT","data":{"last":"64000"}}
/// {"action":"update","args":{"channel":"quote","asset":"BTC","pair":"USDT"},
///  "data":{"rate":"64000"}}
/// ```
///
/// Inbound control, recognised by the presence of [opField]:
///
/// ```json
/// {"op":"subscribe","error":null}
/// {"op":"login","error":"token expired"}
/// ```
///
/// ```dart
/// final codec = JsonSocketCodec<Object?>(
///   parsers: {
///     'ticker': Ticker.fromJson,
///     'candle': Candle.fromJson,
///     'orders': Order.fromJson,
///   },
///   channelKeyFields: {'candle': {'symbol', 'interval'}},
///   fanOutChannels: {'orders'},
/// );
/// ```
class JsonSocketCodec<T> extends SocketCodec<T> {
  /// Creates a codec over [parsers], one per channel the host cares about.
  ///
  /// A frame on a channel with no parser is ignored, so subscribing to a
  /// subset of what a server pushes needs no filtering.
  ///
  /// [keyFields] names the frame fields that, with the channel, identify a
  /// subscription — `{'symbol'}` for most market data. [channelKeyFields]
  /// overrides that per channel, which is how a candle stream gets its
  /// interval into the key.
  ///
  /// [fanOutChannels] are channels whose payloads are routed twice: once to
  /// the key built from the frame, and once to the bare channel with no
  /// arguments. That is the seam for an account feed which arrives per symbol
  /// but which a portfolio screen wants whole.
  ///
  /// [controlOps] narrows which `op` values mark a control frame. Left null,
  /// any frame carrying [opField] is control — the usual case. Name them
  /// explicitly for a server that puts an `op` on its data frames too, and
  /// anything outside the set is decoded as data.
  ///
  /// [errorReader] overrides how a control frame's error is read; see
  /// [JsonErrorReader].
  ///
  /// [heartbeatFrame] is sent by the hub on its heartbeat interval, if one is
  /// configured — `{'op': 'ping'}` for most of these servers.
  JsonSocketCodec({
    required Map<String, JsonPayloadParser<T>> parsers,
    Set<String> keyFields = const <String>{'symbol'},
    Map<String, Set<String>> channelKeyFields = const <String, Set<String>>{},
    Set<String> fanOutChannels = const <String>{},
    Set<String>? controlOps,
    this.errorReader,
    this.opField = 'op',
    this.channelField = 'channel',
    this.dataField = 'data',
    this.argsField = 'args',
    this.errorField = 'error',
    this.subscribeOp = 'subscribe',
    this.unsubscribeOp = 'unsubscribe',
    this.heartbeatFrame,
  })  : _parsers = Map<String, JsonPayloadParser<T>>.unmodifiable(parsers),
        _keyFields = Set<String>.unmodifiable(keyFields),
        _channelKeyFields = Map<String, Set<String>>.unmodifiable(
          channelKeyFields,
        ),
        _fanOutChannels = Set<String>.unmodifiable(fanOutChannels),
        controlOps =
            controlOps == null ? null : Set<String>.unmodifiable(controlOps);

  final Map<String, JsonPayloadParser<T>> _parsers;
  final Set<String> _keyFields;
  final Map<String, Set<String>> _channelKeyFields;
  final Set<String> _fanOutChannels;

  /// The `op` values that mark a control frame, or null for "any of them".
  final Set<String>? controlOps;

  /// How a control frame's error is read, or null to read [errorField].
  final JsonErrorReader? errorReader;

  /// The field naming a control frame's operation. Its presence is what marks
  /// a frame as control rather than data, unless [controlOps] narrows it.
  final String opField;

  /// The field naming the channel a data frame belongs to.
  final String channelField;

  /// The field holding a data frame's payload.
  final String dataField;

  /// The field holding a nested argument map, read when a key field is not at
  /// the top level.
  final String argsField;

  /// The field a control frame reports its error in.
  final String errorField;

  /// The `op` value that subscribes.
  final String subscribeOp;

  /// The `op` value that unsubscribes.
  final String unsubscribeOp;

  /// The keepalive frame, or null for a server that needs none.
  final Map<String, Object?>? heartbeatFrame;

  /// The key fields for [channel].
  Set<String> keyFieldsFor(String channel) =>
      _channelKeyFields[channel] ?? _keyFields;

  @override
  Object? encodeSubscribe(List<SubscriptionKey> keys) =>
      _encodeOp(subscribeOp, keys);

  @override
  Object? encodeUnsubscribe(List<SubscriptionKey> keys) =>
      _encodeOp(unsubscribeOp, keys);

  String _encodeOp(String op, List<SubscriptionKey> keys) =>
      jsonEncode(<String, Object?>{
        opField: op,
        argsField: <Map<String, String>>[
          for (final SubscriptionKey key in keys)
            <String, String>{channelField: key.channel, ...key.args},
        ],
      });

  @override
  Object? encodeHeartbeat() {
    final Map<String, Object?>? frame = heartbeatFrame;
    return frame == null ? null : jsonEncode(frame);
  }

  @override
  SocketDecoded<T> decode(Object? frame) {
    final Map<String, Object?>? json = _asJsonMap(frame);
    if (json == null) {
      return SocketIgnored<T>('not a JSON object', frame: frame);
    }

    final Object? op = json[opField];
    if (op is String && (controlOps?.contains(op) ?? true)) {
      return SocketControl<T>(op, error: _errorIn(json), frame: json);
    }

    final Map<String, Object?>? args = _asJsonMap(json[argsField]);
    final Map<String, Object?>? data = _asJsonMap(json[dataField]);

    final Object? channel = json[channelField] ?? args?[channelField];
    if (channel is! String) {
      return SocketIgnored<T>('no "$channelField" field', frame: json);
    }

    final JsonPayloadParser<T>? parser = _parsers[channel];
    if (parser == null) {
      return SocketIgnored<T>('no parser for channel "$channel"', frame: json);
    }

    final Object? payload = json[dataField];
    if (payload == null) {
      return SocketIgnored<T>('no "$dataField" field', frame: json);
    }

    final Map<String, String?> keyArgs = <String, String?>{};
    for (final String field in keyFieldsFor(channel)) {
      final Object? value = json[field] ?? args?[field] ?? data?[field];
      if (value == null) {
        return SocketIgnored<T>(
          'channel "$channel" needs "$field" to be routed',
          frame: json,
        );
      }
      keyArgs[field] = value.toString();
    }

    final T value;
    try {
      value = parser(payload);
    } on Object catch (error) {
      return SocketIgnored<T>(
        'the "$channel" parser threw: $error',
        frame: json,
      );
    }

    return SocketPayload<T>(
      value,
      keys: <SubscriptionKey>[
        SubscriptionKey(channel, keyArgs),
        if (_fanOutChannels.contains(channel)) SubscriptionKey(channel),
      ],
    );
  }

  Object? _errorIn(Map<String, Object?> json) {
    final JsonErrorReader? reader = errorReader;
    return reader == null ? json[errorField] : reader(json);
  }

  Map<String, Object?>? _asJsonMap(Object? value) {
    if (value is Map<String, Object?>) return value;
    if (value is Map) return value.cast<String, Object?>();
    if (value is String) {
      try {
        final Object? decoded = jsonDecode(value);
        return decoded is Map ? decoded.cast<String, Object?>() : null;
      } on FormatException {
        return null;
      }
    }
    return null;
  }
}
