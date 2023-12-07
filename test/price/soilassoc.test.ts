import fs from 'fs/promises';

import { test } from 'bun:test';

import { load } from 'cheerio';

import { logger, request } from '../lib/utils';

const queue: Array<any> = [];
const baseUrl = 'https://www.soilassociation.org';

test('produce', async () => {
  logger.info(`Fetching ... ${baseUrl}/farmers-growers/market-information/price-data`);
  const response = await request(
    `${baseUrl}/farmers-growers/market-information/price-data`,
  );
  const topics = ['Horticulture', 'Dairy and Egg'];
  const $ = load(response);
  $('.landing-categories a').each((_i, a) => {
    const topic = $(a).find('h2').text().replace('Prices', '').trim();
    if (topics.includes(topic)) {
      logger.info(topic);
      const pathUrl = $(a).attr('href');
      logger.info(pathUrl);
      queue.push({
        topic,
        url: `${baseUrl}${pathUrl}`,
      });
    }
  });
});

test('consume', async () => {
  const { delay } = await import('../lib/utils');
  console.info(delay);
  for (const { topic, url } of queue) {
    const fileDoesExist = await fs.exists(`${topic}.html`);
    logger.info(fileDoesExist);
    let response;
    if (!fileDoesExist) {
      logger.info(`Fetching ... ${url}`);
      response = await request(url);
      await fs.writeFile(`${topic}.html`, response);
    }
    const file = await fs.readFile(`${topic}.html`);
    response = file.toString();
    const $ = load(response);
    const page = $('.grid-section > .column:has(table)');
    const title = page.find('h2').eq(0).text().trim();
    const date = page.find('p').eq(0).text().trim();
    logger.info(title);
    logger.info(date);
    const headers: Array<string> = [];
    const entries: Array<any> = [];
    page.find('table tr').each((i, tr) => {
      const entry: any = {};
      $(tr)
        .find('td')
        .each((j, td) => {
          const text = $(td).text().trim();
          if (i === 0) {
            headers.push(text || `col${j}`);
            return true;
          }

          entry[headers[j]] = text;
        });
      if (Object.keys(entry).length) {
        entries.push(entry);
      }
    });
    logger.info(JSON.stringify(entries.at(-1), null, 2));

    // for (const entry of entries) {
    //   logger.info(JSON.stringify(entry, null, 2));
    // }
    await delay(2000);
  }
});
