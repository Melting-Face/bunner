import {
  expect,
  test,
} from 'bun:test';
import { load } from 'cheerio';

import { range } from 'lodash';

import {
  logger,
  request,
} from '../lib/utils';

test('consume', async () => {
  const response = await request('https://ttta-tapioca.org/%e0%b8%a3%e0%b8%b2%e0%b8%84%e0%b8%b2/?lang=en');
  expect(response).toBeTruthy();
  const $ = load(response);
  const trs = $('table tr');
  const titles: Array<string> = [];
  const subTitles: Array<string> = [];
  trs.eq(0).find('td').each((_i, td) => {
    const colSpan = $(td).attr('colspan');
    logger.info(`colSpan: ${typeof colSpan}`);
    if (colSpan) {
      const title = $(td).text().trim();
      for (const _ of range(colSpan)) {
        titles.push(title);
      }
    }
  });

  trs.eq(1).find('td').each((_i, td) => {
    const subTitle = $(td).text().trim();
    subTitles.push(subTitle);
  });

  trs.slice(2).each((_i, tr) => {
    const tds = $(tr).find('td');
    const date = tds.eq(0).text().trim();
    tds.each((_j, td) => {
      const value = $(td).text().trim();
      const entry = {
        date,
        value,
        title: titles[j],
        subTitle: subTitles[j],
      };
    });
  });

  logger.info(subTitles);
}, 10000);