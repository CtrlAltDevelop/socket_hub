import 'package:socket_hub/socket_hub.dart';
import 'package:test/test.dart';

void main() {
  group('insecureWebSocketWarning', () {
    test('warns on a ws:// URL', () {
      final String? warning = insecureWebSocketWarning(
        Uri.parse('ws://example.com/stream'),
      );

      expect(warning, isNotNull);
      expect(warning, contains('ws'));
      expect(warning, contains('wss'));
    });

    test('is null for a wss:// URL', () {
      expect(
        insecureWebSocketWarning(Uri.parse('wss://example.com/stream')),
        isNull,
      );
    });
  });
}
