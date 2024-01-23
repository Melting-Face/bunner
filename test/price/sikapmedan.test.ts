import { expect, test } from 'bun:test';

import { load } from 'cheerio';
import { snakeCase } from 'lodash';
import moment from 'moment';
import pl from 'nodejs-polars';

import { S3, logger, request } from '../lib/utils';

test('consume', async () => {
  const urls = [
    'https://simpang.pemkomedan.go.id/?menu=harga',
    'https://simpang.pemkomedan.go.id/?menu=harga_ikan',
  ];

  for (const url of urls) {
    const response = await request(url);
    expect(response).toBeTruthy();
    const $ = load(response);

    const rawDate = $('.panel:has(table) h3.panel-title').text().trim();
    const date = moment(rawDate, '.*D MMMM YYYY.*', 'id').format('YYYY-MM-DD');
    const headers: Array<string> = [];

    $('table th').each((_i, th) => {
      const header = snakeCase($(th).text().trim());
      headers.push(header);
    });

    const entries: Array<object> = [];
    $('table tr').each((_i, tr) => {
      const entry: any = { date, source: 'sikapmedan' };
      $(tr)
        .find('td')
        .each((j, td) => {
          entry[headers[j]] = $(td).text().trim();
        });
      entries.push(entry);
    });

    const df = pl.DataFrame(entries);
    logger.info(df.columns);
    const buffer = df.writeParquet();
    await new S3().push(`sikapmedan/${Number(new Date())}.parquet`, buffer);
  }
}, 40000);
