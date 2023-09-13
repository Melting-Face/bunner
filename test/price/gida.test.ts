import {
  expect,
  test,
} from 'bun:test';
import { load } from 'cheerio';
import { insert } from 'sql-bricks';

import request from '../lib/request';

import {
  delay, logger,
} from '../lib/utils';

const date = '2023-09-11';
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

test('produce', async () => {
  for (const { categories, listUrl } of params) {
    const response = await request(listUrl);
    const [tVal, tPas, tUsr, HalTurId] = getUrlParams(response);
    const url = new URL('https://tarim.ibb.istanbul/inc/halfiyatlari/gunluk_fiyatlar.asp');
    url.searchParams.set('tarih', date);
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
  for (const pageUrl of list) {
    const response = await request(pageUrl);
    const $ = load(response);
    $('tr').slice(1).each((_i, tr) => {
      const td = $(tr).find('td');
      const product = $(td).eq(0).text().trim();
      const unit = $(td).eq(1).text().trim();
      const priceMin = $(td).eq(2).text().replace(',', '.').replace(/[^\d.]/, '').trim();
      const priceMax = $(td).eq(3).text().replace(',', '.').replace(/[^\d.]/, '').trim();
      logger.info(`${product}, ${unit}, ${priceMin}, ${priceMax} ${pageUrl}`);
    });
  }
}, 20000);
