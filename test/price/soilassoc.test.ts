import { test } from 'bun:test';

import { load } from 'cheerio';

import { request } from '../lib/utils';

const queue = [];
const baseUrl = 'https://www.soilassociation.org';

test('produce', async () => {
  const response = await request(
    `${baseUrl}/farmers-growers/market-information/price-data`,
  );
  const titles = ['Horticulture Prices', 'Dairy and Egg Prices'];
  const $ = load(response);
});
