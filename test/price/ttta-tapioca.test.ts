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
  let response;
  let tryCount = 3;
  while (tryCount > 0) {
    try {
      response = await request('https://ttta-tapioca.org/%e0%b8%a3%e0%b8%b2%e0%b8%84%e0%b8%b2/?lang=en');
      break;
    } catch (e) {
      logger.warn(e);
      tryCount -= 1;
      await delay(10000);
    }
  }
  expect(response).toBeTruthy();
  const $ = load(response);
  const trs = $('table tr');
  const categories: Array<string> = [];
  const products: Array<string> = [];
  trs.eq(0).find('td').each((_i, td) => {
    const colSpan = Number($(td).attr('colspan'));
    logger.info(`colSpan: ${typeof colSpan}`);
    if (colSpan) {
      const category = $(td).text().trim();
      for (const _ of range(colSpan)) {
        categories.push(category);
      }
    }
  });

  trs.eq(1).find('td').each((_i, td) => {
    const product = $(td).text().trim();
    products.push(product);
  });

  const entries: Array<object> = [];

  trs.slice(2).each((_i, tr) => {
    const tds = $(tr).find('td');
    const date = tds.eq(0).text().trim();
    tds.slice(1).each((j, td) => {
      const value = $(td).text().trim();
      const entry = {
        date,
        value,
        category: categories[j],
        product: products[j],
        uuid: crypto.randomUUID(),
      };
      entries.push(entry);
      logger.info(JSON.stringify(entry, null, 2));
    });
  });

  const ksql = new Ksql('ttta');
  await ksql.insertMany(entries);
  logger.info(trs.eq(1).find('strong').text().trim());
}, 60000);