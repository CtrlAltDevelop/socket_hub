# socket_channels

One WebSocket, many channels. A hub owns the connection and hands out **a
stream per subscription** — listening subscribes, cancelling unsubscribes, and
everything asked for in the same turn of the event loop leaves as a single
frame.

```dart
final hub = SocketChannelHub<Payload>(
  transport: () => WebSocketTransport.connect(Uri.parse('wss://…/stream')),
  codec: JsonSocketCodec<Payload>(parsers: {'ticker': Ticker.fromJson}),
);

// One frame on the wire, not three.
hub.stream(SubscriptionKey('ticker', {'symbol': 'BTCUSDT'})).listen(onTick);
hub.stream(SubscriptionKey('ticker', {'symbol': 'ETHUSDT'})).listen(onTick);
hub.stream(SubscriptionKey('trades', {'symbol': 'BTCUSDT'})).listen(onTrade);
```

Pure Dart, so it works in Flutter apps, server code and CLIs alike. Its only
dependency is `web_socket_channel`, and even that is behind an interface you can
replace.

## Why

Multiplexing one socket across a screenful of widgets is the same problem every
time, and it is always solved twice — once per feed — because the protocol
details are tangled up with the plumbing. The plumbing is what this package is:

- **Reference counting from listeners.** Two widgets watching `BTCUSDT` are one
  subscription. The last one to look away unsubscribes. There is no
  `subscribe`/`unsubscribe` pair to keep balanced by hand, so the usual bug —
  a screen that forgets to unsubscribe and leaves the socket pushing data
  nobody reads — cannot be written.
- **Batching.** Frames are reconciled once per microtask, so a screen opening
  four streams sends one frame, and a symbol switch that closes four and opens
  four sends two.
- **Reconnection that keeps its promises.** Streams survive a drop: a listener
  sees a gap in events, not a done event, and every live subscription is
  re-sent on the new socket. Reconciling from *desired versus sent* rather than
  from a queue of deltas means resubscribing is the same code path as
  subscribing.
- **A seam for tests.** The socket is an interface. `FakeTransport` and
  `MockSocketServer` drive the real hub and the real codec with no network, so
  a mock mode is a different far end rather than a second code path.

What is left — how a subscribe frame is spelled, how an inbound frame names its
channel — is a [`SocketCodec`](#the-codec). One is already written for the
`{"op": "subscribe", "args": [...]}` convention most market-data servers use.

## Install

```yaml
dependencies:
  socket_channels: ^1.0.0
```

Requires Dart 3.13.0 or newer — Flutter 3.47.0 or newer, if you are on Flutter.
There is no `flutter` constraint in `pubspec.yaml`, so the package still
resolves in server and CLI projects with no Flutter SDK installed.

## Subscriptions

A [`SubscriptionKey`] is a channel plus the arguments that narrow it. Two keys
with the same channel and arguments are the same subscription, whoever asked
for them and in whatever order the arguments were written.

```dart
SubscriptionKey('ticker', {'symbol': 'BTCUSDT'});
SubscriptionKey('candle', {'symbol': 'BTCUSDT', 'interval': '15m'});
SubscriptionKey('account_orders');                // no arguments
```

Null values are dropped, so an optional argument needs no conditional at the
call site:

```dart
SubscriptionKey('ticker', {'symbol': symbol, 'interval': interval});
```

Keys are cheap value objects — build them where you need them rather than
caching them. A small helper per channel keeps the strings in one place:

```dart
enum Channel {
  ticker('ticker'),
  candle('candle');

  const Channel(this.wire);
  final String wire;

  SubscriptionKey of({String? symbol, String? interval}) =>
      SubscriptionKey(wire, {'symbol': symbol, 'interval': interval});
}

hub.stream(Channel.candle.of(symbol: 'BTCUSDT', interval: '1h'));
```

### Listening is subscribing

```dart
final sub = hub.stream(Channel.ticker.of(symbol: 'BTCUSDT')).listen(onTick);
// …
await sub.cancel();     // unsubscribes, if nothing else is listening
```

Calling `stream` before the socket is open is fine: the subscription is
remembered and sent as soon as one is ready.

### Holding one open with nothing listening

For the case where the data has to keep flowing while no widget is watching —
priming a cache, holding a symbol across a page transition — take a lease:

```dart
final lease = hub.subscribeAll([
  Channel.ticker.of(symbol: 'BTCUSDT'),
  Channel.candle.of(symbol: 'BTCUSDT', interval: '15m'),
]);
// …later
lease.close();
```

A lease and a listener both count, so a leased key with two listeners is one
wire subscription and a reference count of three.

### The last value

With `retainLatest: true`, the most recent payload per subscription is kept and
handed to each new listener straight away, so a widget built between two ticks
does not sit empty until the next one:

```dart
final hub = SocketChannelHub<Payload>(
  transport: …,
  codec: …,
  retainLatest: true,
);

hub.latest(Channel.ticker.of(symbol: 'BTCUSDT'));   // or read it directly
```

Nothing is cached for a key no caller holds, so a server that pushes hundreds
of symbols cannot grow the cache without bound.

## The codec

`SocketCodec` is the whole protocol: three methods to translate frames, and an
optional handshake.

| Member | What it does |
| --- | --- |
| `encodeSubscribe(keys)` | The frame that subscribes to `keys`. Never empty, and already batched |
| `encodeUnsubscribe(keys)` | The frame that unsubscribes. Same |
| `decode(frame)` | Reads one inbound frame into a `SocketPayload`, a `SocketControl`, or a `SocketIgnored` |
| `handshake(socket)` | Runs after the socket opens, before any subscription. Defaults to nothing |
| `encodeHeartbeat()` | The keepalive frame, or null |

`decode` must not throw — return `SocketIgnored` for anything unexpected. A
codec that throws anyway is treated as having ignored the frame.

### The ready-made one

`JsonSocketCodec` speaks the convention most exchange sockets use. Give it a
parser per channel and it handles both directions:

```dart
final codec = JsonSocketCodec<Payload>(
  parsers: {
    'ticker': Ticker.fromJson,
    'candle': Candle.fromJson,
    'account_orders': Order.fromJson,
  },
  channelKeyFields: {'candle': {'symbol', 'interval'}},
  fanOutChannels: {'account_orders'},
  heartbeatFrame: {'op': 'ping'},
);
```

Outbound it writes `{"op":"subscribe","args":[{"channel":…,…}]}`. Inbound it
reads a data frame's key fields from the top level, from a nested `args` map,
or from inside `data` — whichever carries them, which covers all three shapes
in the wild:

```json
{"channel":"ticker","symbol":"BTCUSDT","data":{"last":"64000"}}
{"channel":"candle","symbol":"BTCUSDT","data":{"interval":"15m","close":"1"}}
{"action":"update","args":{"channel":"quote","asset":"BTC","pair":"USDT"},
 "data":{"rate":"64000"}}
```

A frame carrying `op` is control rather than data. Every field name is
configurable — `opField`, `channelField`, `dataField`, `argsField`,
`errorField`, `subscribeOp`, `unsubscribeOp` — for a server that spells them
its own way.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `parsers` | — | Channel to payload parser. A channel with no parser is ignored, so subscribing to a subset needs no filtering |
| `keyFields` | `{'symbol'}` | The frame fields that, with the channel, identify a subscription |
| `channelKeyFields` | `{}` | Per-channel override of `keyFields` |
| `fanOutChannels` | `{}` | Channels also routed to the bare channel key — see below |
| `heartbeatFrame` | null | The keepalive frame |

### Fanning one frame out

An account update arrives for one symbol, but a portfolio screen wants all of
them. Return both keys and the hub routes the payload twice:

```dart
fanOutChannels: {'account_orders'},

hub.stream(SubscriptionKey('account_orders', {'symbol': 'BTCUSDT'}));  // one
hub.stream(SubscriptionKey('account_orders'));                         // all
```

In a codec of your own, that is just a longer `keys` list:

```dart
return SocketPayload(order, keys: [
  SubscriptionKey('account_orders', {'symbol': symbol}),
  SubscriptionKey('account_orders'),
]);
```

### Logging in first

Private channels usually sit behind a login, and subscribing before it lands is
rejected. Override `handshake`: it runs after the socket opens and *before* any
subscription frame, so the ordering is guaranteed rather than hoped for.

```dart
@override
Future<void> handshake(SocketHandshake socket) async {
  final token = await readToken();
  if (token == null) return;                 // public channels only
  socket.send(jsonEncode({'op': 'login', 'token': token}));
  await socket.expect('login');              // throws if the server refuses
}
```

Throwing — including `expect` timing out — fails the connection attempt and
hands it to the reconnect policy, which is the behaviour you want for an
expired token: the next attempt reads a fresh one.

Call `expect` in the same turn as the `send` it answers, or rely on the buffer:
replies that arrive during the handshake are held, so `expect` finds one that
has already landed.

## Reconnecting

A dropped socket is reopened on a backoff, and every live subscription is
re-sent once it is ready. Streams stay open throughout.

```dart
reconnectPolicy: const ReconnectPolicy(),                  // the default
reconnectPolicy: const ReconnectPolicy(maxAttempts: 5),
reconnectPolicy: const ReconnectPolicy.none(),             // never retry
```

| Field | Default | Meaning |
| --- | --- | --- |
| `initialDelay` | 500 ms | The wait before the first attempt |
| `maxDelay` | 30 s | The ceiling, however many have failed |
| `factor` | `2` | What each attempt multiplies the delay by. `1` is a fixed delay |
| `jitter` | `0.2` | Fraction to randomise each delay by, so a fleet dropped together does not return in lockstep |
| `maxAttempts` | null | How many tries before giving up. Null retries forever |

When the policy does give up, each open stream receives the last connection
error and then closes, and the hub moves to `closed` for good. With the default
policy that never happens.

### Watching the connection

```dart
hub.connectionStates.listen((state) => banner.visible = !state.isReady);
```

| `SocketConnectionState` | Meaning |
| --- | --- |
| `idle` | No socket, none being opened. The state a hub starts in, and returns to after `disconnect()` |
| `connecting` | Opening, or running the handshake. Subscriptions asked for now are held and sent when it is ready |
| `ready` | Open, handshake done, subscriptions live |
| `reconnecting` | Dropped; a retry is scheduled |
| `closed` | Disposed, or the policy ran out of attempts. Terminal |

`controlFrames` carries every control frame the codec reported — a rejected
subscribe, an expired session — for trouble a subscription stream cannot show.

### Keepalive

Servers that drop idle connections need a ping, and a socket that has silently
died looks exactly like a quiet one until you ask:

```dart
final hub = SocketChannelHub<Payload>(
  transport: …,
  codec: …,                              // with an encodeHeartbeat frame
  heartbeatInterval: const Duration(seconds: 20),
  idleTimeout: const Duration(seconds: 60),
);
```

`heartbeatInterval` sends the codec's heartbeat frame on a timer.
`idleTimeout` treats a socket that has delivered nothing for that long as dead
and reconnects it. Both are off by default.

### Backgrounding

`disconnect()` closes the socket and stops retrying but keeps every
subscription and stream, so `connect()` restores exactly what was there:

```dart
// AppLifecycleState.paused
await hub.disconnect();
// AppLifecycleState.resumed
await hub.connect();
```

`dispose()` is the other one: it closes the streams too, and the hub cannot be
reused.

## The hub

| Parameter | Default | Meaning |
| --- | --- | --- |
| `transport` | — | Opens a socket. Called once per attempt, so each reconnect gets a fresh one |
| `codec` | — | The protocol |
| `reconnectPolicy` | exponential | See above |
| `retainLatest` | `false` | Keep and replay the last payload per subscription |
| `autoConnect` | `true` | Whether the first subscription opens the socket on its own |
| `heartbeatInterval` | null | How often to send the codec's heartbeat |
| `idleTimeout` | null | How long a silent socket may stay open |
| `log` | null | Where the hub reports what it is doing |
| `random` | null | The source of backoff jitter. Seed it to make a test reproducible |

| Member | What it does |
| --- | --- |
| `stream(key)` | The payloads for `key`. Listening subscribes, cancelling unsubscribes |
| `subscribe(key)` / `subscribeAll(keys)` | A lease holding subscriptions open with no listener |
| `connect()` / `disconnect()` / `dispose()` | Lifecycle. `connect` does not throw on a failed attempt |
| `whenReady()` | Completes the next time the hub is ready |
| `connectionState` / `connectionStates` | Where it is now, and every change |
| `controlFrames` | Control frames the codec reported |
| `activeSubscriptions` / `wireSubscriptions` | What callers want, and what the server has been told |
| `refCount(key)` / `latest(key)` | Diagnostics, and the retained payload |
| `send(frame)` | Sends a raw frame, bypassing the codec |

## Testing against it

`package:socket_channels/socket_channels_testing.dart` has the two pieces that
make a hub testable without a server.

**`FakeTransport`** is a socket with nothing behind it. Everything the far end
would do is a method call, and everything the hub sends is recorded:

```dart
final transport = FakeTransport();
final hub = SocketChannelHub<Payload>(transport: () => transport, codec: codec);

hub.stream(SubscriptionKey('ticker', {'symbol': 'BTC'})).listen(events.add);
await hub.whenReady();
expect(transport.sent.single, contains('"channel":"ticker"'));

transport.emit('{"channel":"ticker","symbol":"BTC","data":{"last":1}}');
await transport.closeByPeer();          // watch the hub come back
transport.emitError(StateError('rude'));
```

**`MockSocketServer`** goes one further: it reads the subscribe frames the hub
sends and pushes generated payloads back for whatever is subscribed. The hub,
the codec and the app's wiring are all the real ones — only the far end is
invented, which is what makes it usable as a mock mode and not just a test
double.

```dart
final server = MockSocketServer(
  tick: const Duration(days: 1),         // driven by pump(), not a clock
  build: (key, tick) => switch (key.channel) {
    'ticker' => {'symbol': key.args['symbol'], 'last': 64000 + tick},
    _ => null,                           // nothing to say on this channel
  },
);
final hub = SocketChannelHub<Payload>(transport: server.open, codec: codec);

await hub.whenReady();
server.pump();                           // one round of payloads
expect(server.subscriptions, hasLength(1));
```

Leave `tick` long in tests and call `pump()`, rather than sleeping. In a mock
mode, set it to the cadence you want and let it run.

## Example

A market-data socket end to end — login handshake, four channels, a fan-out, a
dropped socket that comes back — is in
[`example/socket_channels_example.dart`](example/socket_channels_example.dart).
It runs as it stands, with no network:

```bash
dart run example/socket_channels_example.dart
```

## Licence

MIT — see [LICENSE](LICENSE).

[`SubscriptionKey`]: https://pub.dev/documentation/socket_channels/latest/socket_channels/SubscriptionKey-class.html
