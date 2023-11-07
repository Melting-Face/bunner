import { test } from 'bun:test';
import { load } from 'cheerio';

import {
  logger,
  request,
} from '../lib/utils';
import moment from 'moment';

test('consume', async () => {
  const response = await request('http://www.targowisko.mewat.pl/notowania.xml');
  const date = moment().format('YYYY-MM-DD');
  const $ = load(response);
  const headers: Array<string> = [];
  $('TOP > *').each((_i, el) => {
    const header = $(el).text().trim();
    headers.push(header);
  });

  logger.info($('TOP > *').text().trim());

  $('HURT').each((_i, hurt) => {
    $(hurt).find('> *').each((j, el) => {
      logger.info($(el).text().trim());
    });
  });
});
