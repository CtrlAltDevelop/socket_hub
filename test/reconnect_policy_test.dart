import 'dart:math';

import 'package:socket_channels/socket_channels.dart';
import 'package:test/test.dart';

void main() {
  group('ReconnectPolicy', () {
    const ReconnectPolicy plain = ReconnectPolicy(
      initialDelay: Duration(milliseconds: 100),
      maxDelay: Duration(seconds: 2),
      jitter: 0,
    );

    test('doubles each attempt, starting at the initial delay', () {
      expect(plain.delayFor(1), const Duration(milliseconds: 100));
      expect(plain.delayFor(2), const Duration(milliseconds: 200));
      expect(plain.delayFor(3), const Duration(milliseconds: 400));
      expect(plain.delayFor(4), const Duration(milliseconds: 800));
    });

    test('stops growing at maxDelay', () {
      expect(plain.delayFor(20), const Duration(seconds: 2));
      expect(plain.delayFor(200), const Duration(seconds: 2));
    });

    test('factor 1 gives a fixed delay', () {
      const ReconnectPolicy fixed = ReconnectPolicy(
        initialDelay: Duration(milliseconds: 250),
        factor: 1,
        jitter: 0,
      );
      expect(fixed.delayFor(1), const Duration(milliseconds: 250));
      expect(fixed.delayFor(9), const Duration(milliseconds: 250));
    });

    test('maxAttempts gives up by returning null', () {
      const ReconnectPolicy limited = ReconnectPolicy(maxAttempts: 3);
      expect(limited.delayFor(3), isNotNull);
      expect(limited.delayFor(4), isNull);
    });

    test('ReconnectPolicy.none never reconnects', () {
      expect(const ReconnectPolicy.none().delayFor(1), isNull);
    });

    test('jitter stays inside its fraction of the delay', () {
      const ReconnectPolicy jittered = ReconnectPolicy(
        initialDelay: Duration(milliseconds: 1000),
        maxDelay: Duration(milliseconds: 1000),
        jitter: 0.25,
      );
      final Random random = Random(7);

      for (int i = 0; i < 500; i++) {
        final Duration delay = jittered.delayFor(1, random: random)!;
        expect(delay.inMicroseconds, greaterThanOrEqualTo(750 * 1000));
        expect(delay.inMicroseconds, lessThanOrEqualTo(1250 * 1000));
      }
    });

    test('a seeded random makes the backoff reproducible', () {
      const ReconnectPolicy jittered = ReconnectPolicy();
      expect(
        jittered.delayFor(3, random: Random(42)),
        jittered.delayFor(3, random: Random(42)),
      );
    });

    test('jitter cannot push a delay below zero', () {
      const ReconnectPolicy jittered = ReconnectPolicy(jitter: 1);
      final Random random = Random(1);
      for (int i = 0; i < 200; i++) {
        expect(jittered.delayFor(1, random: random)!.isNegative, isFalse);
      }
    });
  });
}
