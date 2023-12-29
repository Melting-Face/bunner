import { expect, test } from 'bun:test';
import { load } from 'cheerio';
import moment from 'moment';

import { delay, logger, request } from '../lib/utils';

const date = '2023-12-11';
const THAI_SOLAR_CALENDER = 543;

const htmls: Array<string> = [];

test('produce', async () => {
  const workDate = moment(date).add(THAI_SOLAR_CALENDER, 'year');
  let response = await request(
    'https://www.oae.go.th/view/1/%E0%B8%A3%E0%B8%B2%E0%B8%84%E0%B8%B2%E0%B8%AA%E0%B8%B4%E0%B8%99%E0%B8%84%E0%B9%89%E0%B8%B2%E0%B9%80%E0%B8%81%E0%B8%A9%E0%B8%95%E0%B8%A3/TH-TH',
  );
  expect(response).toBeTruthy();
  let $ = load(response);
  let url =
    $('a[title*="ข้าวหอมมะลิ 105"],a[title*="ข้าวเปลือกเจ้าหอมมะลิ 105"]').attr(
      'href',
    ) || '';
  let href;
  let pageDate = '';
  do {
    response = await request(url);
    $ = load(response);
    pageDate = $('.post_meta').text().trim();
    logger.info(pageDate);
    if (moment(pageDate, 'DD MMMM YYYY', 'th').isSameOrBefore(workDate)) {
      break;
    }
    logger.info(url);
    htmls.push(response);
    logger.info(pageDate);
    href = $('.post_desc a').attr('href');
    expect(href).toBeTruthy();
    url = `https://www.oae.go.th${href}`;
    await delay(1000);
  } while (pageDate && href);
}, 90000);

test('consume', async () => {
  for (const html of htmls) {
    const $ = load(html);
    const year = $('table th').eq(0).text().trim();
    const product = $('table th').eq(1).text().trim();
    logger.info(`${year}, ${product}`);
    let month: string;
    const entries = [];
    $('table tr').each((_i, tr) => {
      const tds = $(tr).find('td');
      const monthCol = tds.eq(0).text().trim();
      const day = tds.eq(1).text().trim();

      if (!month && monthCol) {
        month = monthCol;
      }
      const entry: any = {
        day,
        month,
      };
      logger.info('PR2');
      tds.slice(2).each((j, td) => {
        const text = $(td).text().trim();
        entry[`col${j}`] = text;
      });
      logger.info(JSON.stringify(entry, null, 2));
    });
  }
});
