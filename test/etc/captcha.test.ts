import fs from 'fs/promises';

import {
  afterAll,
  beforeAll,
  test,
} from 'bun:test';
import { range } from 'lodash';
import {
  Browser,
  Page,
} from 'puppeteer';
import puppeteer from 'puppeteer-extra';
import StealthPlugin from 'puppeteer-extra-plugin-stealth';

import request, {
  delay,
  logger,
} from '../lib/utils';

let browser: Browser;
let page: Page;

beforeAll(async () => {
  puppeteer.use(StealthPlugin());
  browser = await puppeteer.launch({
    headless: false,
    defaultViewport: null,
  });

  page = await browser.newPage();
});

test('Get Image', async () => {
  await page.goto('https://data.nbd.ltd/en/login?r=%2Fen%2Fmarket-analysis%2Fdetails%2Fvietnam_im%3Fdid%3DJXnJNbHsro95vPx5AwkskMaW9nXvgvKjckzf9NZOKv0%26iid%3DNBDX2H170669928%26eid%3DNBDDIY315438475');
  for (const idx of range(56, 1000)) {
    await page.waitForSelector('.input-group-addon img', { visible: true });
    const imageSrc = await page.$eval('.input-group-addon img', (el) => el.getAttribute('src'));
    const buffer = await request({
      type: 'buffer',
      url: imageSrc,
    });
    await fs.writeFile(`image/${idx}.png`, buffer);
    await delay(500);
    await page.reload();
  }
}, 13000000);

afterAll(async () => {
  if (browser) {
    try {
      await browser.close();
    } catch (e) {
      logger.error(e);
    }
  }
});
