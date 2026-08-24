/// The identity of one subscription on a multiplexed socket.
///
/// A key is a channel name plus the arguments that narrow it — a symbol, an
/// interval, an asset pair, whatever the server's protocol asks for. Two keys
/// with the same channel and the same arguments are the same subscription, so
/// they share a stream and a single wire subscription no matter how many
/// callers ask for them.
///
/// ```dart
/// SubscriptionKey('ticker', {'symbol': 'BTCUSDT'});
/// SubscriptionKey('candle', {'symbol': 'BTCUSDT', 'interval': '15m'});
/// SubscriptionKey('account_orders');            // no arguments
/// ```
///
/// Argument order does not matter: the map is sorted before [id] is built, so
/// `{'symbol': 'BTC', 'interval': '1m'}` and `{'interval': '1m', 'symbol':
/// 'BTC'}` are one key.
final class SubscriptionKey {
  /// Creates a key for [channel], narrowed by [args].
  ///
  /// Entries in [args] with a null value are dropped, so an optional argument
  /// can be passed straight through without a conditional at the call site.
  SubscriptionKey(this.channel, [Map<String, String?> args = const {}])
    : args = Map.unmodifiable(<String, String>{
        for (final MapEntry<String, String?> e in args.entries)
          if (e.value != null) e.key: e.value!,
      });

  /// The channel name, as the server spells it.
  final String channel;

  /// The arguments narrowing [channel]. Unmodifiable.
  final Map<String, String> args;

  /// A canonical string form, unique per (channel, args) pair.
  ///
  /// Used as the map key for this subscription's stream and reference count,
  /// and stable across runs, so it is safe to log or to persist.
  String get id {
    if (args.isEmpty) return channel;
    final List<String> names = args.keys.toList()..sort();
    final StringBuffer buffer = StringBuffer(channel);
    for (final String name in names) {
      buffer
        ..write('|')
        ..write(name)
        ..write('=')
        ..write(args[name]);
    }
    return buffer.toString();
  }

  /// This key's [args] merged with [extra], as a new key on the same channel.
  ///
  /// Values in [extra] win. A null value removes that argument, which is how
  /// a narrower key is widened:
  ///
  /// ```dart
  /// // 'candle|interval=15m|symbol=BTC' -> 'candle|symbol=BTC'
  /// key.copyWith({'interval': null});
  /// ```
  SubscriptionKey copyWith(Map<String, String?> extra) =>
      SubscriptionKey(channel, <String, String?>{...args, ...extra});

  @override
  String toString() => 'SubscriptionKey($id)';

  @override
  bool operator ==(Object other) => other is SubscriptionKey && other.id == id;

  @override
  int get hashCode => id.hashCode;
}
