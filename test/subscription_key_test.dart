import 'package:socket_channels/socket_channels.dart';
import 'package:test/test.dart';

void main() {
  group('SubscriptionKey', () {
    test('a bare channel is its own id', () {
      expect(SubscriptionKey('ticker').id, 'ticker');
    });

    test('args are sorted into the id, so call order cannot matter', () {
      final SubscriptionKey a = SubscriptionKey('candle', <String, String>{
        'symbol': 'BTCUSDT',
        'interval': '15m',
      });
      final SubscriptionKey b = SubscriptionKey('candle', <String, String>{
        'interval': '15m',
        'symbol': 'BTCUSDT',
      });

      expect(a.id, 'candle|interval=15m|symbol=BTCUSDT');
      expect(a.id, b.id);
      expect(a, b);
      expect(a.hashCode, b.hashCode);
    });

    test('null args are dropped, so an optional one needs no conditional', () {
      final SubscriptionKey key = SubscriptionKey('ticker', <String, String?>{
        'symbol': 'BTCUSDT',
        'interval': null,
      });

      expect(key.args, <String, String>{'symbol': 'BTCUSDT'});
      expect(key.id, 'ticker|symbol=BTCUSDT');
    });

    test('the same channel with different args is a different key', () {
      expect(
        SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}),
        isNot(SubscriptionKey('ticker', <String, String>{'symbol': 'ETH'})),
      );
      expect(
        SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}),
        isNot(SubscriptionKey('ticker')),
      );
    });

    test('args cannot be modified through the key', () {
      final SubscriptionKey key = SubscriptionKey('ticker', <String, String>{
        'symbol': 'BTC',
      });
      expect(() => key.args['symbol'] = 'ETH', throwsUnsupportedError);
    });

    test('copyWith adds, replaces and — with null — removes an arg', () {
      final SubscriptionKey key = SubscriptionKey('candle', <String, String>{
        'symbol': 'BTC',
        'interval': '15m',
      });

      expect(
        key.copyWith(<String, String>{'interval': '1h'}).id,
        'candle|interval=1h|symbol=BTC',
      );
      expect(
        key.copyWith(<String, String?>{'interval': null}).id,
        'candle|symbol=BTC',
      );
      expect(key.copyWith(<String, String>{'venue': 'spot'}).args.length, 3);
    });

    test('works as a map key and a set member', () {
      final Map<SubscriptionKey, int> counts = <SubscriptionKey, int>{
        SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}): 1,
      };
      counts[SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'})] = 2;

      expect(counts, hasLength(1));
      expect(counts.values.single, 2);
    });

    test('toString names the id', () {
      expect(
        SubscriptionKey('ticker', <String, String>{'symbol': 'BTC'}).toString(),
        'SubscriptionKey(ticker|symbol=BTC)',
      );
    });
  });
}
