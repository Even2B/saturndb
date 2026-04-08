import 'dart:async';
import 'dart:convert';
import 'dart:io';

typedef EventHandler = void Function(String stream, Map<String, dynamic> payload);

bool _matchesPattern(String pattern, String stream) {
  if (pattern == stream) return true;
  if (pattern.endsWith('*')) return stream.startsWith(pattern.substring(0, pattern.length - 1));
  return false;
}

class SaturnDBClient {
  final String _host;
  final int    _port;
  final String _token;

  Socket?  _socket;
  String   _buffer          = '';
  bool     _closing         = false;
  Duration _reconnectDelay  = const Duration(milliseconds: 500);

  final Map<String, EventHandler> _handlers    = {};
  final StreamController<String>  _lineStream  = StreamController.broadcast();
  StreamSubscription?             _subscription;

  SaturnDBClient({String host = '127.0.0.1', int port = 7379, String token = ''})
      : _host  = host,
        _port  = port,
        _token = token;

  Future<void> connect() async {
    _socket = await Socket.connect(_host, _port);
    _socket!.setOption(SocketOption.tcpNoDelay, true);
    _reconnectDelay = const Duration(milliseconds: 500);

    _subscription = _socket!
        .cast<List<int>>()
        .transform(utf8.decoder)
        .listen(_onData, onDone: _onClose, onError: (_) => _onClose());

    if (_token.isNotEmpty) await _send('AUTH $_token');
  }

  Future<void> emit(String stream, Map<String, dynamic> payload) async {
    await _send('EMIT $stream ${jsonEncode(payload)}');
  }

  Future<Map<String, dynamic>?> get(String stream) async {
    final response = await _send('GET $stream');
    if (response.startsWith('VALUE ')) {
      return jsonDecode(response.substring(6)) as Map<String, dynamic>;
    }
    return null;
  }

  Future<List<Map<String, dynamic>>> since(String stream, int ts) async {
    final response = await _send('SINCE $stream $ts');
    if (response == 'EMPTY') return [];
    return response
        .split('\n')
        .where((l) => l.startsWith('EVENT '))
        .map((l) {
          final rest    = l.substring(6);
          final spaceAt = rest.indexOf(' ');
          return {
            'stream':  rest.substring(0, spaceAt),
            'payload': jsonDecode(rest.substring(spaceAt + 1)),
          };
        })
        .toList();
  }

  Future<void> watch(String pattern, EventHandler handler) async {
    _handlers[pattern] = handler;
    await _send('WATCH $pattern');
  }

  Future<String> ping() => _send('PING');

  Future<void> disconnect() async {
    _closing = true;
    await _subscription?.cancel();
    await _socket?.close();
  }

  Future<String> _send(String line) async {
    _socket!.write('$line\n');
    await for (final response in _lineStream.stream) {
      if (response.startsWith('EVENT ')) {
        _dispatch(response);
        continue;
      }
      return response;
    }
    throw StateError('connection closed');
  }

  void _onData(String chunk) {
    _buffer += chunk;
    final lines = _buffer.split('\n');
    _buffer = lines.removeLast();

    for (final line in lines) {
      final trimmed = line.trim();
      if (trimmed.isEmpty) continue;
      _lineStream.add(trimmed);
    }
  }

  void _dispatch(String line) {
    final rest    = line.substring(6);
    final spaceAt = rest.indexOf(' ');
    final stream  = rest.substring(0, spaceAt);
    final payload = jsonDecode(rest.substring(spaceAt + 1)) as Map<String, dynamic>;

    for (final entry in _handlers.entries) {
      if (_matchesPattern(entry.key, stream)) {
        entry.value(stream, payload);
      }
    }
  }

  void _onClose() {
    if (_closing) return;
    _scheduleReconnect();
  }

  Future<void> _scheduleReconnect() async {
    print('[saturn] disconnected — reconnecting in ${_reconnectDelay.inMilliseconds}ms');
    await Future.delayed(_reconnectDelay);
    _reconnectDelay = Duration(
      milliseconds: (_reconnectDelay.inMilliseconds * 2).clamp(0, 10000),
    );
    try {
      await connect();
      for (final pattern in _handlers.keys) {
        await _send('WATCH $pattern');
      }
      print('[saturn] reconnected');
    } catch (_) {
      _scheduleReconnect();
    }
  }
}
