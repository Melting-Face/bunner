import {
  expect,
  test,
} from 'bun:test';

import { load } from 'cheerio';
import { range } from 'lodash';

import {
  Ksql,
  delay,
  logger,
  request,
} from '../lib/utils';

test('consume', async () => {
  for (const index of range(1, 2)) {
    const body = {
      selectedMarket: [],
      selectedProductGroup: [index],
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
    const entries = data.map((el: any) => {
      el.uuid = crypto.randomUUID();
      return el;
    });
    console.info(entries[0]);
    const ksql = new Ksql('simummuangmarket');
    await ksql.insertMany(entries);
  }
}, 120000);