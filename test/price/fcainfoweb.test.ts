import {
  afterAll,
  beforeAll,
  test,
} from 'bun:test';
import fs from 'fs/promises';
import { load } from 'cheerio';
import moment from 'moment';
import puppeteer, {
  Browser,
  Page,
} from 'puppeteer';

import {
  delay,
  Ksql,
  logger,
} from '../lib/utils';

const date = '2023-10-31';

let page: Page;
let browser: Browser;
let html: any;

// beforeAll(async () => {
//   browser = await puppeteer.launch({
//     defaultViewport: null,
//     headless: false,
//   });
//   page = await browser.newPage();
// });

// test('produce', async () => {
//   const url = 'https://fcainfoweb.nic.in/Reports/Report_Menu_Web.aspx';
//   await page.goto(url, { timeout: 60000 });
//   await page.waitForSelector('#ctl00_MainContent_Ddl_Rpt_type', {
//     visible: true,
//     timeout: 10000,
//   });
//   await page.select('#ctl00_MainContent_Ddl_Rpt_type', 'Wholesale');
//   await page.waitForSelector('#ctl00_MainContent_Ddl_Rpt_type', {
//     visible: true,
//     timeout: 10000,
//   });
//   await page.select('#ctl00_MainContent_ddl_Language', 'Hindi');
//   await page.focus('#ctl00_MainContent_Rbl_Rpt_type_0');
//   await delay(250);
//   await page.click('#ctl00_MainContent_Rbl_Rpt_type_0');
//   await page.waitForSelector('#ctl00_MainContent_Ddl_Rpt_Option0', {
//     visible: true,
//     timeout: 10000,
//   });
//   await page.select('#ctl00_MainContent_Ddl_Rpt_Option0', 'Daily Prices');
//   await page.waitForSelector('#ctl00_MainContent_Txt_FrmDate', { visible: true });
//   const [year, mon, day] = date.split('-');
//   const formattedDate = `${day}/${mon}/${year}`;
//   await page.type('#ctl00_MainContent_Txt_FrmDate', formattedDate);
//   await page.keyboard.press('Enter');
//   await page.waitForSelector('#Panel1', {
//     visible: true,
//     timeout: 120000,
//   });
//   await delay(30000);
//   html = await page.content();
// }, 600000);

test('consume', async () => {
  html = await fs.readFile('fcainfoweb.html');
  const $ = load(html);
  const workDate = $('table[width="95%"] td[align="left"]').text().trim();
  const rawUnit = $('table[width="95%"] td[align="right"]').text().trim();
  const tableHead = $('.MyCssClass thead tr');
  const tableBody = $('.MyCssClass tbody tr');
  const tableHeaderText = tableHead.text().trim();
  logger.info(workDate);
  logger.info(tableHeaderText === 'राज्यचावलगेहूँआटा (गेहूं)चना दालतूर / अरहर दालउड़द दालमूंग दालमसूर दालचीनीदूध @मूंगफली तेल (पैक)सरसों तेल (पैक)वनस्पति (पैक)सोया तेल (पैक)सूरजमुखी तेल (पैक)पाम तेल (पैक)गुड़खुली चायनमक पैक *आलूप्याजटमाटर');
  logger.info(rawUnit === 'यूनिट: (₹/क्विंटल)');

  const entries: Array<object> = [];
  const products: Array<string> = [];
  $(tableHead).find('th').slice(1).each((_i, th) => {
    const product = $(th).text().trim();
    products.push(product);
  });

  $(tableBody).each((_i, tr) => {
    const tds = $(tr).find('td');
    const region = $(tds).eq(0).text().trim();
    if (['औसत मूल्य', 'अधिकतम मूल्य', 'न्यूनतम मूल्य', 'मॉडल मूल्य'].includes(region)) {
      return true;
    }

    $(tds).slice(1).each((j, td) => {
      const entry = {
        uuid: crypto.randomUUID(),
        region,
        product: products[j],
        price: $(td).text().trim(),
      };
      entries.push(entry);
    });
  });

  const ksql = new Ksql('fcainfoweb');
  await ksql.insertMany(entries);
}, 60000);

// afterAll(async () => {
//   if (browser) {
//     try {
//       await browser.close();
//     } catch (e) {
//       logger.error(`Closing browser error: ${e}`);
//     }
//   }
// });