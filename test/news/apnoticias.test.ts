import {
  expect,
  test,
} from 'bun:test';
import { load } from 'cheerio';
import moment from 'moment';
import { insert } from 'sql-bricks';

import request from '../lib/request';
import {
  delay,
  logger,
} from '../lib/utils';

const SOURCE = 'apnoticias';
const limit = 1;
const workDate = '2023-09-19';
const host = 'https://www.apnoticias.pe';
const newsList: any = [];
const pathUrls = [
  '/actualidad?p={offset}',
  '/mundo?p={offset}',
  '/politica?p={offset}',
  '/economia?p={offset}',
];

test('produce', async () => {
  for (const pathUrl of pathUrls) {
    let offset = 1;
    let dataDoesExisted = true;
    while (dataDoesExisted) {
      const url = `${host}${pathUrl}`.replace('{offset}', `${offset}`);
      logger.info(`Fetching ... ${url}`);
      const response = await request(url);
      expect(response).toBeTruthy();

      const news: any = [];
      const $ = load(response);
      $('.section-content .content').each((_i, section) => {
        const articleRawDate = $(section).find('time.date').text().trim();
        const articleMomentDate = moment(articleRawDate, 'D [de] MMMM YYYY', 'es');
        const articleUrl = $(section).find('.title a').attr('href');
        if (articleMomentDate.isSameOrAfter(workDate)) {
          news.push({
            DATE: articleMomentDate.format('YYYY-MM-DD'),
            url: articleUrl,
          });
        }
      });

      dataDoesExisted = !!news.length;
      if (dataDoesExisted) {
        newsList.push(...news);
      }

      await delay(1000);
      offset += limit;
    }
  }
}, 60000);

test('consume', async () => {
  for (const { DATE, url } of newsList) {
    logger.info(`Fetching ... ${url}`);
    const response = await request(url);
    expect(response).toBeTruthy();
    const $ = load(response);
    const bodyList: Array<string> = [];
    $('.post-content p').each((_i, p) => {
      const bodyText = $(p).text().trim();
      bodyList.push(bodyText);
    });
    const TITLE = $('meta[property="og:title"]').attr('content');
    const SUMMARY = $('meta[property="og:description"]').attr('content');
    const IMAGE = $('meta[property="og:image"]').attr('content');
    const BODY = bodyList.join('\n');
    const news = {
      DATE,
      BODY,
      IMAGE,
      SUMMARY,
      TITLE,
      SOURCE,
      URL: url,
    };
    const ksql = `${insert('NEWS', news).toString()};`;
    await request({
      url: 'http://localhost:8088/ksql',
      method: 'POST',
      headers: { Accept: 'application/vnd.ksql.v1+json' },
      body: {
        ksql,
        streamsProperties: {},
      },
    });
    await delay(1000);
  }
}, 260000);
