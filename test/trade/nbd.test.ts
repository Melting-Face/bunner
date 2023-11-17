import {
  beforeAll,
  test,
} from 'bun:test';
import puppeteer, {
  Browser,
  Page,
} from 'puppeteer';

let browser: Browser;
let page: Page;

beforeAll(async () => {
  browser = await puppeteer.launch({
    defaultViewport: null,
    headless: false,
  });
  page = await browser.newPage();
});

test('login', async () => {
  await page.goto('https://en.nbd.ltd');
  await page.waitForXPath('//a[contains(text(), "Client Login")]');
  const webElements = await page.$x('//a[contains(text(), "Client Login")]');
  if (webElements.length) {
    await webElements[0].click();
  }
}, 30000);
