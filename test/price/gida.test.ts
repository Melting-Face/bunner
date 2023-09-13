import {
  afterAll,
  beforeAll,
  expect,
  test,
} from 'bun:test';
import { load } from 'cheerio';
import { range } from 'lodash';
import { insert } from 'sql-bricks';

import request from '../lib/request';

import {
  delay,
  logger,
} from '../lib/utils';

const date = '2022-09-11';
const urls = [
  'https://tarim.ibb.istanbul/tr/istatistik/178/hal-fiyatlari.html',
  'https://tarim.ibb.istanbul/tr/istatistik/124/hal-fiyatlari.html',
];

test('produce', async () => {
  for (const url of urls) {
    const response = await request(url);
    const $ = load(response);
    $('script[type="text/javascript"]:not([src])').each((_i, script) => {
      const scriptContent = $(script).text().trim();
      if (!scriptContent.includes('ButtonEvents')) {
        return true;
      }

      const scriptRows = scriptContent.split('\n');
      for (const row of scriptRows) {
        if (row.includes('obj.tVal')) {
          logger.info(row);
        }
      }
    });
    await delay(1000);
  }
}, 30000);
