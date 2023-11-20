import {
  expect,
  test,
} from 'bun:test';

import {
  Ksql,
  delay,
  logger,
  request,
} from '../lib/utils';

const queue: Array<number> = [];

test('produce', async () => {
  const response = await request({
    url: 'https://www.simummuangmarket.com/api/product-group/frontend/list',
    method: 'POST',
    type: 'json',
  });
  expect(response).toBeTruthy();
  const productGroups = ['ผัก', 'ผลไม้', 'พืชไร่', 'ของแห้งของดอง', 'ของสดของชำ'];
  for (const { GroupNameEN, Id } of response) {
    if (productGroups.includes(GroupNameEN)) {
      queue.push(Id);
    }
  }
});

test('consume', async () => {
  for (const productGroup of queue) {
    console.info(productGroup);
    const body = {
      selectedMarket: [],
      selectedProductGroup: [productGroup],
      sortOrder: 'PriceL desc',
      fromRow: 1,
      toRow: 24,
    };
    let response = await request({
      body,
      url: 'https://www.simummuangmarket.com/api/product/frontend/search',
      method: 'POST',
      type: 'json',
    });
    logger.info(response.totalRecords);
    await delay(1000);

    body.toRow = response.totalRecords;
    response = await request({
      body,
      url: 'https://www.simummuangmarket.com/api/product/frontend/search',
      method: 'POST',
      type: 'json',
    });
    expect(response).toBeTruthy();
    await delay(1000);
    const { data } = response;
    console.info(data[0]);
    const ksql = new Ksql('simummuangmarket');
    await ksql.insertMany(data);
  }
}, 120000);