import fs from 'fs/promises';

import {
  beforeAll,
  test,
} from 'bun:test';
import { range } from 'lodash';

import { load } from 'cheerio';
import pl from 'nodejs-polars';
import xlsx, { Sheet } from 'xlsx';

import request from '../lib/request';
import {
  delay,
  logger,
} from '../lib/utils';

let response: any;
const baseUrl = 'https://unstats.un.org';
const filenames: Array<string> = [];
const urls: Array<string> = [];

beforeAll(async () => {
  response = await request(`${baseUrl}/unsd/classifications/Econ`);
});

test('Get urls', async () => {
  const $ = load(response);
  let index = 0;
  $('div#corresp-hs .panel-heading').each((i, el) => {
    const title = $(el).text().trim();
    if (title === 'HS 1992 - 2022') {
      logger.info(title);
      index = i;
    }
  });

  $('div#corresp-hs .panel-body').eq(index).find('a').each((_i, a) => {
    const href = $(a).attr('href');
    let url = `${baseUrl}${href}`;
    if (url === decodeURI(url)) {
      url = encodeURI(url);
    }
    urls.push(url);
  });
});

test('Get files', async () => {
  for (const url of urls) {
    const filename = url.split('/').pop() || '';
    filenames.push(filename);
    const isExisted = await fs.exists(`xlsx/${filename}`);
    if (!isExisted) {
      const buffer = await request({
        url,
        type: 'buffer',
      });
      await fs.writeFile(`xlsx/${filename}`, buffer);
      await delay(700);
    }
  }
}, 60000);

test('Read files', async () => {
  for (const filename of filenames) {
    const workbook = xlsx.readFile(`xlsx/${filename}`);
    const sheetNames = workbook.SheetNames;
    const sheetName = sheetNames.find((el) => el.includes('Correlation')) || '';
    const sheet: Sheet = workbook.Sheets[sheetName];
    logger.info('=================================');
    logger.info(filename);
    logger.info(sheetName);
    logger.info(sheet.A1?.v);
    logger.info(sheet.B1?.v);
    let csv = xlsx.utils.sheet_to_csv(sheet);
    const csvRows = csv.split('\n') || [];
    if (sheet.A1?.v === 'Between') {
      csvRows.shift();
    }
    if (sheet.B1?.v?.includes('Correlation')) {
      for (const _i in range(6)) {
        csvRows.shift();
      }
    }

    logger.info(csvRows[0]);
    csv = csvRows.join('\n');
    const df = pl.readCSV(csv);
    df.writeCSV(`csv/${filename}.csv`);
    logger.info('=================================');
  }
}, 60000);