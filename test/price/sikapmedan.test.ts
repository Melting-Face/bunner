import { test } from 'bun:test';

test('consume', async () => {
  const urls = [
    'https://simpang.pemkomedan.go.id/?menu=harga',
    'https://simpang.pemkomedan.go.id/?menu=harga_ikan',
  ];

  for (const url of urls) {
    const response = await request(url);
  }
});