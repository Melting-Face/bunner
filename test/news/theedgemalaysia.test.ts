import {
  afterAll,
  beforeAll,
  expect,
  test,
} from 'bun:test';
import { load } from 'cheerio';
import { Transaction } from 'kafkajs';
import moment from 'moment';

import request from '../lib/request';
import {
  delay,
  logger,
  producer,
} from '../lib/utils';

const limit = 10;
const workDate = '2023-09-02';
const host = 'https://theedgemalaysia.com';
const newsList: any = [];
const pathUrls = [
  '/api/loadMoreCategories?offset={offset}&categories=economy',
  // '/api/loadMoreCategories?offset={offset}&categories=corporate',
  // '/api/loadMoreCategories?offset={offset}&categories=court',
  // '/api/loadMoreOption?offset={offset}&option=politics',
];
beforeAll(async () => {
  await producer.connect();
});

test('produce', async () => {
  for (const pathUrl of pathUrls) {
    let offset = 0;
    let dataDoesExisted = true;
    while (dataDoesExisted) {
      const url = `${host}${pathUrl}`.replace('{offset}', `${offset}`);
      logger.info(`Fetching ... ${url}`);
      const { results = [] } = await request({
        url,
        type: 'json',
      });
      expect(results.length).toBeTruthy();

      const news = [];
      for (const { alias, created } of results) {
        const momentDate = moment(created);
        if (momentDate.isSameOrAfter(workDate)) {
          news.push({
            date: momentDate.format('YYYY-MM-DD'),
            url: `${host}/${alias}`,
          });
        }
      }

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
  for (const { date, url } of newsList) {
    logger.info(`Fetching ... ${url}`);
    const response = await request(url);
    expect(response).toBeTruthy();
    const $ = load(response);
    const bodyList: Array<string> = [];
    $('[class^="news-detail_newsTextDataWrap"] p').each((_i, p) => {
      const bodyText = $(p).text().trim();
      bodyList.push(bodyText);
    });
    const title = $('meta[property="og:title"]').attr('content');
    const summary = $('meta[property="og:description"]').attr('content');
    const image = $('meta[property="og:image"]').attr('content');
    const body = bodyList.join('\n');
    const news = {
      date,
      body,
      image,
      summary,
      title,
      url,
    };
    await producer.send({
      topic: 'news',
      messages: [{ value: JSON.stringify(news) }],
    });
    await delay(1000);
  }
}, 60000);

afterAll(async () => {
  await producer.disconnect();
});
