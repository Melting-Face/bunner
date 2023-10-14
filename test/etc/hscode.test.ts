import fs from 'fs/promises';

import { test } from 'bun:test';

import pl from 'nodejs-polars';
import xlsx from 'xlsx';

import request from '../lib/request';
import { logger } from '../lib/utils';

test('hscode-1', async () => {
  const { results } = await request({
    type: 'json',
    url: 'https://comtradeapi.un.org/files/v1/app/reference/HS.json',
  });

  await fs.writeFile('HS.json', JSON.stringify(results, null, 2));
  let df = pl.DataFrame(results);
  df.writeParquet('HS.parquet');

  const ids = [];
  for (const { id } of results) {
    if (/^[0-4]\d*|TOTAL/.test(id)) {
      ids.push(id);
      logger.info(id);
    }
  }
  df = pl.DataFrame({ id: ids });
  df.writeParquet('hscodeFromUnComtrade.parquet');
}, 30000);

// https://unstats.un.org/unsd/classifications/Econ/tables/HS2022toHS2012ConversionAndCorrelationTables.xlsx
test('hscode-2022', async () => {
  const buffer = await request({
    type: 'buffer',
    url: 'https://unstats.un.org/unsd/classifications/Econ/tables/HS2022toHS2012ConversionAndCorrelationTables.xlsx',
  });

  await fs.writeFile('HS2022_2012.xlsx', buffer);

  const workbook = xlsx.read(buffer);
  const sheet = workbook.Sheets['HS2022-HS2012 Correlations'];
  const items: any = xlsx.utils.sheet_to_json(sheet);
  const entries = [];
  for (const item of items) {
    if (/^[0-4]\d*|TOTAL/.test(item.Between) && item.__EMPTY_1 === '1:1') {
      const entry = {
        HS2022: item.Between,
        HS2012: item.__EMPTY,
      };
      entries.push(entry);
    }
  }

  await fs.writeFile('HS2022_2012.json', JSON.stringify(entries, null, 2));
  const df = pl.DataFrame(entries);
  df.writeParquet('HS2022_2012.parquet');
}, 30000);

// https://unstats.un.org/unsd/classifications/Econ/tables/HS2017toHS2012ConversionAndCorrelationTables.xlsx
test('hscode-2017', async () => {
  const buffer = await request({
    type: 'buffer',
    url: 'https://unstats.un.org/unsd/classifications/Econ/tables/HS2017toHS2012ConversionAndCorrelationTables.xlsx',
  });

  await fs.writeFile('HS2017_2012.xlsx', buffer);

  const workbook = xlsx.read(buffer);
  const sheet = workbook.Sheets['Correlation HS17-HS12'];
  const items: any = xlsx.utils.sheet_to_json(sheet);
  const entries = [];
  for (const item of items) {
    if (/^[0-4]\d*|TOTAL/.test(item.Between) && item.__EMPTY_2 === '1:1') {
      const entry = {
        HS2022: item.Between,
        HS2012: item.__EMPTY_1,
      };
      entries.push(entry);
    }
  }

  await fs.writeFile('HS2017_2012.json', JSON.stringify(entries, null, 2));
  const df = pl.DataFrame(entries);
  df.writeParquet('HS2017_2012.parquet');
}, 30000);