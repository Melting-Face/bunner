import {
  expect,
  test,
} from 'bun:test';

import request from '../lib/request';
import {
  delay,
  logger,
} from '../lib/utils';

const limit = 10;
const host = 'https://theedgemalaysia.com';
const newsList = [];
const pathUrls = [
  '/api/loadMoreCategories?offset={offset}&categories=economy',
  '/api/loadMoreCategories?offset={offset}&categories=corporate',
  '/api/loadMoreCategories?offset={offset}&categories=court',
  '/api/loadMoreOption?offset={offset}&option=politics',
];

test('produce', async () => {
  for (const pathUrl of pathUrls) {
    let offset = 0;
    let dataDoesExisted = true;
    while (dataDoesExisted) {
      const url = `${host}${pathUrl}`.replace('{offset}', `${offset}`);
      logger.info(url);
      // const response = await request(url);
      await delay(1000);
      offset += limit;
    }
  }
});

test('consume', () => {

});
