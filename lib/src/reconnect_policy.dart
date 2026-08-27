import 'dart:math';

/// How long to wait before each reconnect attempt, and when to stop trying.
///
/// The default is exponential backoff from 500 ms to 30 s with 20% jitter and
/// no attempt limit — a socket that keeps failing keeps being retried, at a
/// rate that will not hammer a server coming back from an outage.
///
/// ```dart
/// const ReconnectPolicy()                       // the default above
/// const ReconnectPolicy.none()                  // never reconnect
/// const ReconnectPolicy(maxAttempts: 5)         // give up after five tries
/// const ReconnectPolicy(factor: 1)              // a fixed 500 ms delay
/// ```
class ReconnectPolicy {
  /// Creates a backoff policy.
  ///
  /// [initialDelay] is the wait before the first attempt; each later attempt
  /// multiplies it by [factor], capped at [maxDelay]. [jitter] spreads each
  /// delay by up to that fraction in either direction, so a fleet of clients
  /// dropped by the same outage does not return in lockstep.
  ///
  /// [maxAttempts] null means retry forever.
  const ReconnectPolicy({
    this.initialDelay = const Duration(milliseconds: 500),
    this.maxDelay = const Duration(seconds: 30),
    this.factor = 2,
    this.jitter = 0.2,
    this.maxAttempts,
  })  : assert(factor >= 1, 'factor below 1 would shrink the delay'),
        assert(jitter >= 0 && jitter <= 1, 'jitter is a fraction of the delay'),
        assert(
          maxAttempts == null || maxAttempts > 0,
          'maxAttempts 0 means never reconnect — use ReconnectPolicy.none()',
        );

  /// A policy that never reconnects: the first drop closes the hub.
  const ReconnectPolicy.none()
      : initialDelay = Duration.zero,
        maxDelay = Duration.zero,
        factor = 1,
        jitter = 0,
        maxAttempts = 0;

  /// The wait before the first reconnect attempt.
  final Duration initialDelay;

  /// The ceiling on the wait, however many attempts have failed.
  final Duration maxDelay;

  /// What each attempt multiplies the previous delay by.
  final num factor;

  /// The fraction of each delay to randomise by, in either direction.
  final double jitter;

  /// How many attempts to make before giving up, or null for no limit.
  final int? maxAttempts;

  /// The wait before attempt number [attempt], or null to stop trying.
  ///
  /// [attempt] is 1-based: the first reconnect after a drop is attempt 1.
  /// Pass a seeded [random] to make the jitter reproducible in tests.
  Duration? delayFor(int attempt, {Random? random}) {
    assert(attempt >= 1, 'attempts are 1-based');
    final int? limit = maxAttempts;
    if (limit != null && attempt > limit) return null;

    // pow() with two ints does integer arithmetic, which wraps to zero
    // once the exponent passes 63. Doubles saturate to infinity instead,
    // and min() below caps that at maxDelay.
    final double raw = initialDelay.inMicroseconds *
        pow(factor.toDouble(), attempt - 1).toDouble();
    final double capped = min(raw, maxDelay.inMicroseconds.toDouble());
    if (jitter == 0) return Duration(microseconds: capped.round());

    // nextDouble() is in [0, 1), so this spreads the delay across
    // [capped * (1 - jitter), capped * (1 + jitter)).
    final double spread = ((random ?? _random).nextDouble() * 2 - 1) * jitter;
    final double jittered = capped * (1 + spread);
    return Duration(microseconds: max(0, jittered).round());
  }

  static final Random _random = Random();
}
