import {
  expect,
  test,
} from 'bun:test';

import request from '../lib/request';
import {
  delay,
  logger,
} from '../lib/utils';

const startOffset = 0;
const limit = 10;
const host = 'https://theedgemalaysia.com';
const newsList = [];
const pathUrls = [
  '/api/loadMoreCategories?offset={offset}&categories=economy',
  '/api/loadMoreCategories?offset={offset}&categories=corporate',
  '/api/loadMoreCategories?offset={offset}&categories=court',
  '/api/loadMoreOption?offset={offset}&option=politics',
];

test('produce', () => {
  for (const pathUrl of pathUrls) {
    let offset = startOffset;
    logger.info(pathUrl);
    let dataDoesExisted = true;
    while (dataDoesExisted) {

    }
  }
});

test('consume', () => {

});
