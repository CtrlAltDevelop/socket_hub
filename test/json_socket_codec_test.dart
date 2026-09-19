import 'dart:convert';

import 'package:socket_hub/socket_hub.dart';
import 'package:test/test.dart';

/// A payload type that is easy to assert on.
class Tick {
  Tick(this.raw);

  factory Tick.fromJson(Object? data) =>
      Tick((data! as Map<String, Object?>).cast<String, Object?>());

  final Map<String, Object?> raw;
}

void main() {
  JsonSocketCodec<Tick> codecUnder({
    Set<String> fanOut = const <String>{},
    Map<String, Set<String>> channelKeyFields = const <String, Set<String>>{},
  }) => JsonSocketCodec<Tick>(
    parsers: <String, JsonPayloadParser<Tick>>{
      'ticker': Tick.fromJson,
      'candle': Tick.fromJson,
      'orders': Tick.fromJson,
      'quote': Tick.fromJson,
    },
    channelKeyFields: channelKeyFields,
    fanOutChannels: fanOut,
    heartbeatFrame: const <String, Object?>{'op': 'ping'},
  );

  group('encoding', () {
    test('batches every key into one subscribe frame', () {
      final Object? frame = codecUnder().encodeSubscribe(<SubscriptionKey>[
        SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}),
        SubscriptionKey('candle', <String, String>{
          'symbol': 'BTC',
          'interval': '15m',
        }),
      ]);

      expect(jsonDecode(frame! as String), <String, Object?>{
        'op': 'subscribe',
        'args': <Object?>[
          <String, Object?>{'channel': 'ticker', 'symbol': 'BTC'},
          <String, Object?>{
            'channel': 'candle',
            'interval': '15m',
            'symbol': 'BTC',
          },
        ],
      });
    });

    test('a bare channel encodes with no arguments', () {
      final Object? frame = codecUnder().encodeUnsubscribe(<SubscriptionKey>[
        SubscriptionKey('orders'),
      ]);

      expect(jsonDecode(frame! as String), <String, Object?>{
        'op': 'unsubscribe',
        'args': <Object?>[
          <String, Object?>{'channel': 'orders'},
        ],
      });
    });

    test('the heartbeat is the configured frame, or nothing', () {
      expect(
        jsonDecode(codecUnder().encodeHeartbeat()! as String),
        <String, Object?>{'op': 'ping'},
      );
      expect(
        JsonSocketCodec<Tick>(
          parsers: <String, JsonPayloadParser<Tick>>{},
        ).encodeHeartbeat(),
        isNull,
      );
    });
  });

  group('decoding data', () {
    test('routes a frame whose key field is at the top level', () {
      final SocketDecoded<Tick> decoded = codecUnder().decode(
        '{"channel":"ticker","symbol":"BTC","data":{"last":64000}}',
      );

      expect(
        decoded,
        isA<SocketPayload<Tick>>()
            .having(
              (SocketPayload<Tick> p) => p.keys.single.id,
              'key',
              'ticker|symbol=BTC',
            )
            .having(
              (SocketPayload<Tick> p) => p.value.raw['last'],
              'payload',
              64000,
            ),
      );
    });

    test('reads a key field out of data when the top level lacks it', () {
      final SocketDecoded<Tick> decoded =
          codecUnder(
            channelKeyFields: <String, Set<String>>{
              'candle': <String>{'symbol', 'interval'},
            },
          ).decode(
            '{"channel":"candle","symbol":"BTC",'
            '"data":{"interval":"15m","close":1}}',
          );

      expect(
        (decoded as SocketPayload<Tick>).keys.single.id,
        'candle|interval=15m|symbol=BTC',
      );
    });

    test('reads the channel and key fields out of a nested args map', () {
      // The shape a convert/quote feed uses: nothing identifying at the top
      // level except the action.
      final SocketDecoded<Tick> decoded =
          codecUnder(
            channelKeyFields: <String, Set<String>>{
              'quote': <String>{'asset', 'pair'},
            },
          ).decode(
            '{"action":"update","args":{"channel":"quote","asset":"BTC",'
            '"pair":"USDT"},"data":{"rate":"64000"}}',
          );

      expect(
        (decoded as SocketPayload<Tick>).keys.single.id,
        'quote|asset=BTC|pair=USDT',
      );
    });

    test('fans a payload out to the bare channel as well', () {
      final SocketDecoded<Tick> decoded = codecUnder(
        fanOut: <String>{'orders'},
      ).decode('{"channel":"orders","symbol":"BTC","data":{"id":1}}');

      expect(
        (decoded as SocketPayload<Tick>).keys.map((SubscriptionKey k) => k.id),
        <String>['orders|symbol=BTC', 'orders'],
      );
    });

    test('accepts an already-decoded map as well as a string', () {
      final SocketDecoded<Tick> decoded = codecUnder().decode(<String, Object?>{
        'channel': 'ticker',
        'symbol': 'BTC',
        'data': <String, Object?>{'last': 1},
      });
      expect(decoded, isA<SocketPayload<Tick>>());
    });

    test('a numeric key field is stringified rather than dropped', () {
      final SocketDecoded<Tick> decoded = codecUnder(
        channelKeyFields: <String, Set<String>>{
          'ticker': <String>{'id'},
        },
      ).decode('{"channel":"ticker","id":7,"data":{"last":1}}');

      expect((decoded as SocketPayload<Tick>).keys.single.id, 'ticker|id=7');
    });
  });

  group('decoding control', () {
    test('an op field marks the frame as control', () {
      final SocketDecoded<Tick> decoded = codecUnder().decode(
        '{"op":"subscribe","error":null}',
      );

      expect(
        decoded,
        isA<SocketControl<Tick>>()
            .having((SocketControl<Tick> c) => c.op, 'op', 'subscribe')
            .having((SocketControl<Tick> c) => c.isError, 'isError', isFalse),
      );
    });

    test('carries the error the server reported', () {
      final SocketControl<Tick> control =
          codecUnder().decode('{"op":"login","error":"token expired"}')
              as SocketControl<Tick>;

      expect(control.isError, isTrue);
      expect(control.error, 'token expired');
      expect(control.toString(), contains('token expired'));
    });
  });

  group('decoding what cannot be routed', () {
    test('ignores a frame that is not a JSON object', () {
      expect(codecUnder().decode('not json'), isA<SocketIgnored<Tick>>());
      expect(codecUnder().decode(null), isA<SocketIgnored<Tick>>());
      expect(codecUnder().decode(<int>[1, 2]), isA<SocketIgnored<Tick>>());
    });

    test('ignores a channel with no parser, so a subset can be consumed', () {
      expect(
        codecUnder().decode('{"channel":"depth","symbol":"BTC","data":{}}'),
        isA<SocketIgnored<Tick>>().having(
          (SocketIgnored<Tick> i) => i.reason,
          'reason',
          contains('depth'),
        ),
      );
    });

    test('ignores a frame missing the field its key needs', () {
      expect(
        codecUnder().decode('{"channel":"ticker","data":{"last":1}}'),
        isA<SocketIgnored<Tick>>().having(
          (SocketIgnored<Tick> i) => i.reason,
          'reason',
          contains('symbol'),
        ),
      );
    });

    test('ignores a frame with no data', () {
      expect(
        codecUnder().decode('{"channel":"ticker","symbol":"BTC"}'),
        isA<SocketIgnored<Tick>>(),
      );
    });

    test('a throwing parser is ignored rather than propagated', () {
      final JsonSocketCodec<Tick> codec = JsonSocketCodec<Tick>(
        parsers: <String, JsonPayloadParser<Tick>>{
          'ticker': (Object? _) => throw const FormatException('bad field'),
        },
      );

      expect(
        codec.decode('{"channel":"ticker","symbol":"BTC","data":{}}'),
        isA<SocketIgnored<Tick>>().having(
          (SocketIgnored<Tick> i) => i.reason,
          'reason',
          contains('bad field'),
        ),
      );
    });
  });

  test(
    'field names are configurable for a server that spells them its way',
    () {
      final JsonSocketCodec<Tick> codec = JsonSocketCodec<Tick>(
        parsers: <String, JsonPayloadParser<Tick>>{'ticker': Tick.fromJson},
        opField: 'event',
        channelField: 'topic',
        dataField: 'payload',
        subscribeOp: 'sub',
      );

      expect(
        jsonDecode(
          codec.encodeSubscribe(<SubscriptionKey>[
                SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}),
              ])!
              as String,
        ),
        <String, Object?>{
          'event': 'sub',
          'args': <Object?>[
            <String, Object?>{'topic': 'ticker', 'symbol': 'BTC'},
          ],
        },
      );
      expect(
        codec.decode('{"topic":"ticker","symbol":"BTC","payload":{"last":1}}'),
        isA<SocketPayload<Tick>>(),
      );
      expect(codec.decode('{"event":"sub"}'), isA<SocketControl<Tick>>());
    },
  );

  test('controlOps keeps a data frame that carries an op', () {
    final JsonSocketCodec<Tick> codec = JsonSocketCodec<Tick>(
      parsers: <String, JsonPayloadParser<Tick>>{'ticker': Tick.fromJson},
      controlOps: const <String>{'subscribe', 'login'},
    );

    expect(
      codec.decode(
        '{"op":"update","channel":"ticker","symbol":"BTC","data":{"last":1}}',
      ),
      isA<SocketPayload<Tick>>(),
    );
    expect(codec.decode('{"op":"login"}'), isA<SocketControl<Tick>>());
  });

  test('errorReader decides whether a control frame failed', () {
    final JsonSocketCodec<Tick> codec = JsonSocketCodec<Tick>(
      parsers: <String, JsonPayloadParser<Tick>>{'ticker': Tick.fromJson},
      errorReader: (Map<String, Object?> frame) =>
          frame['success'] == false ? frame['msg'] : null,
    );

    expect(
      codec.decode('{"op":"login","success":false,"msg":"expired"}'),
      isA<SocketControl<Tick>>().having(
        (SocketControl<Tick> c) => c.error,
        'error',
        'expired',
      ),
    );
    expect(
      codec.decode('{"op":"login","success":true,"error":"ignored"}'),
      isA<SocketControl<Tick>>().having(
        (SocketControl<Tick> c) => c.isError,
        'isError',
        isFalse,
      ),
    );
  });
}
