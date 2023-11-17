import {
  expect,
  test,
} from 'bun:test';
import { load } from 'cheerio';
import moment from 'moment';

import {
  Ksql,
  logger,
  request,
} from '../lib/utils';

test('consume', async () => {
  const url = 'https://www.soilassociation.org/farmers-growers/market-information/price-data/arable-price-data/';
  const response = await request(url);
  expect(response).toBeTruthy();
  const $ = load(response);

  const h3 = $('div:has(> table) h3');
  const title = h3.eq(0).text().trim();
  let rawDate = h3.eq(1).text().trim();
  [, rawDate] = rawDate.split('/');
  const date = moment(rawDate, 'MMMM YYYY').format('YYYY-MM-DD');
  logger.info(date);
  expect(title).toBe('Organic Arable Prices');

  const subTitles: Array<string> = [];
  $('div:has(> table) table td').slice(0, 2).each((_i, td) => {
    const subTitle = $(td).text().trim();
    subTitles.push(subTitle);
  });

  expect(subTitles.length).toBe(2);
  expect(subTitles[0]).toBe('Commodity');
  expect(subTitles[1]).toBe('£ / Tonne');
  const entries: Array<object> = [];
  $('div:has(> table) table tr').slice(1).each((_j, tr) => {
    const td = $(tr).find('td');
    const product = td.eq(0).text().trim();
    const price = td.eq(1).text().trim();
    entries.push({
      date,
      product,
      price,
      uuid: crypto.randomUUID(),
    });
  });

  const ksql = new Ksql('soilassociation');
  await ksql.insertMany(entries);
}, 20000);
