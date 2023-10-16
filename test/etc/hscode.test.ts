import fs from 'fs/promises';

import { test } from 'bun:test';

import { load } from 'cheerio';
import pl from 'nodejs-polars';
import xlsx from 'xlsx';

import request from '../lib/request';
import { logger } from '../lib/utils';

test('Get urls', async () => {
  const response = await request('https://unstats.un.org/unsd/classifications/Econ');
  const $ = load(response);
  let index = 0;
  $('div#corresp-hs .panel-heading').each((i, el) => {
    const title = $(el).text().trim();
    if (title === 'HS 1992 - 2022') {
      logger.info(title);
      index = i;
    }
  });

  $('div#corresp-hs .panel-body').eq(index).find('a').each((i, a) => {
    const href = $(a).attr('href');
    logger.info(href);
  });
});
