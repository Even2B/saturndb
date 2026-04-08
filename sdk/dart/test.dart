import 'saturn.dart';

void main() async {
  final client = SaturnDBClient(host: '127.0.0.1', port: 7379, token: 'saturn-admin-secret');
  await client.connect();
  print('connected to saturn');

  print('ping → ${await client.ping()}');

  await client.watch('users:*', (stream, payload) {
    print('[watch] $stream → $payload');
  });

  await client.emit('users:1', {'name': 'evan', 'status': 'online'});
  await client.emit('users:2', {'name': 'john', 'status': 'away'});
  await client.emit('orders:1', {'total': 99});

  final val = await client.get('users:1');
  print('get users:1 → $val');

  await Future.delayed(const Duration(milliseconds: 200));
  await client.disconnect();
}
