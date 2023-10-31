import { createHash } from 'crypto';

import { test } from 'bun:test';
import { load } from 'cheerio';

import {
  Ksql,
  delay,
  logger,
  request,
} from '../lib/utils';

const SOURCE = 'Gida';
const DATE = '2023-09-11';
const params = [
  {
    listUrl: 'https://tarim.ibb.istanbul/tr/istatistik/178/hal-fiyatlari.html',
    categories: [1, 2, 3, 4],
  },
  {
    listUrl: 'https://tarim.ibb.istanbul/tr/istatistik/124/hal-fiyatlari.html',
    categories: [5, 6, 7],
  },
];

const list: Array<string> = [];

function getUrlParams(html: string) {
  let tVal = '';
  let tPas = '';
  let tUsr = '';
  let HalTurId = '';

  const $ = load(html);
  $('script[type="text/javascript"]:not([src])').each((_i, script) => {
    const scriptContent = $(script).text().trim();
    if (!scriptContent.includes('ButtonEvents')) {
      return true;
    }

    const scriptRows = scriptContent.split('\n');
    for (const row of scriptRows) {
      if (row.includes('obj.tVal')) {
        [, tVal] = row.match(/["](\S+)["]/) || [];
      }
      if (row.includes('obj.tPas')) {
        [, tPas] = row.match(/["](\S+)["]/) || [];
      }
      if (row.includes('obj.tUsr')) {
        [, tUsr] = row.match(/["](\S+)["]/) || [];
      }
      if (row.includes('obj.HalTurId')) {
        [, HalTurId] = row.match(/["](\S+)["]/) || [];
      }
    }
  });
  return [tVal, tPas, tUsr, HalTurId];
}
// beforeAll(async () => {
//   const ksql = `
// CREATE TABLE IF NOT EXISTS PRICES (
//     HASH VARCHAR PRIMARY KEY,
//     SOURCE VARCHAR,
//     DATE VARCHAR,
//     UNIT VARCHAR,
//     PAGEURL VARCHAR,
//     PRICEAVG DOUBLE,
//     PRICEMAX DOUBLE,
//     PRICEMIN DOUBLE,
//     PRODUCT VARCHAR
// ) WITH (
//     kafka_topic='prices',
//     value_format='json',
//     partitions=1
// );
// `;
//   const response = await request({
//     url: 'http://localhost:8088/ksql',
//     method: 'POST',
//     headers: { Accept: 'application/vnd.ksql.v1+json' },
//     body: {
//       ksql,
//       streamsProperties: {},
//     },
//   });
//   logger.info(`Response: ${response}`);
// });

test('produce', async () => {
  for (const { categories, listUrl } of params) {
    const response = await request(listUrl);
    const [tVal, tPas, tUsr, HalTurId] = getUrlParams(response);
    const url = new URL('https://tarim.ibb.istanbul/inc/halfiyatlari/gunluk_fiyatlar.asp');
    url.searchParams.set('tarih', DATE);
    url.searchParams.set('tVal', tVal);
    url.searchParams.set('tPas', tPas);
    url.searchParams.set('tUsr', tUsr);
    url.searchParams.set('HalTurId', HalTurId);
    for (const category of categories) {
      url.searchParams.set('kategori', `${category}`);
      list.push(url.toString());
    }
    await delay(1000);
  }
}, 30000);

test('consume', async () => {
  const entries: Array<object> = [];
  for (const PAGEURL of list) {
    logger.info(PAGEURL);
    const response = await request(PAGEURL);
    const $ = load(response);
    $('tr:has(td)').each((_i, tr) => {
      const td = $(tr).find('td');
      const PRODUCT = td.eq(0).text().trim();
      const UNIT = td.eq(1).text().trim();
      const PRICEMIN = td.eq(2).text();
      const PRICEMAX = td.eq(3).text();
      const hash = createHash('md5').update(`${SOURCE}${PRODUCT}${UNIT}${DATE}${PAGEURL}`).digest('hex');
      const entry = {
        hash,
        DATE,
        UNIT,
        PAGEURL,
        PRICEMAX,
        PRICEMIN,
        PRODUCT,
        SOURCE,
      };
      entries.push(entry);
    });
  }
  const ksql = new Ksql(SOURCE);
  await ksql.insertMany(entries);
}, 100000);
