import fs from 'fs/promises';
import {
  Readable,
  Writable,
} from 'stream';

import {
  beforeAll,
  test,
} from 'bun:test';
import unzipper from 'unzipper';

import request from '../lib/request';
import { logger } from '../lib/utils';

const fileNames = [
  'Prices_E_All_Data_(Normalized)',
  'Production_Crops_Livestock_E_All_Data_(Normalized)',
];

interface BufferWithName {
  [key: string]: Buffer;
}

const bufferWithName: BufferWithName = {};

function bulkCsvToJson(
  data: string | Buffer,
  columnSeparator = ',',
  rowSeparator = '\n',
  headerSeparator = ',',
  preprocessForRow = ((row: string) => row),
) {
  if (!data) {
    throw new Error('Data is required');
  }

  if (typeof data !== 'string' && !Buffer.isBuffer(data)) {
    throw new Error('Type Error: Only Buffer or String is available');
  }

  const entries: any = [];
  const reader = new Readable();
  reader.on('readable', () => {
    let row = '';
    let text = '';
    let chunk = reader.read(100);

    const headers: Array<string> = [];
    while (chunk !== null) {
      text += chunk;
      chunk = reader.read(100);
      // if (text.includes(rowSeparator)) {
      //   [row, text] = text.split(rowSeparator);
      //   if (!row) {
      //     continue;
      //   }
      //   if (!headers.length) {
      //     headers.push(...row.split(headerSeparator));
      //     continue;
      //   }
      //   const entry: any = {};
      //   const columns = preprocessForRow(row).split(columnSeparator);
      //   for (const index in columns) {
      //     entry[headers[index]] = columns[index];
      //   }
      //   entries.push(entry);
      // }
    }
  });
  reader.push(data);
  return entries;
}

beforeAll(async () => {
  for (const fileName of fileNames) {
    try {
      await fs.access(`${fileName}.csv`);
    } catch (e) {
      const url = `https://fenixservices.fao.org/faostat/static/bulkdownloads${fileName}.zip`;
      logger.info(`Fetching ... ${url}`);
      const response = await request({
        url,
        type: 'buffer',
      });
      const directory = await unzipper.Open.buffer(response);
      for (const file of directory.files) {
        const { path } = file;
        if (`${fileName}.csv` === path) {
          const fileBuffer = await file.buffer();
          await fs.writeFile(path, fileBuffer);
        }
      }
    }
    const stats = await fs.stat(`${fileName}.csv`);
    logger.info(`${fileName} file size`);
    logger.info(`${stats.size / 1024 / 1024} MB`);
    const buffer = await fs.readFile(`${fileName}.csv`);
    bufferWithName[fileName] = buffer;
  }
});

test('csv parsing', () => {
  for (const fileName of fileNames) {
    logger.info(`${fileName}`);
    logger.info(`Memory: ${process.memoryUsage().heapTotal / 1024 / 1024} MB`);
    const buffer = bufferWithName[fileName];

    logger.info('Get Buffer data');
    logger.info(`Memory: ${process.memoryUsage().heapTotal / 1024 / 1024} MB`);

    const entries = bulkCsvToJson(buffer, '","', '\r\n', ',', ((row) => row.replace(/^["]|["]$/g, '')));
    logger.info('Convert csv');
    logger.info(`Memory: ${process.memoryUsage().heapTotal / 1024 / 1024} MB`);

    logger.info(JSON.stringify(entries.slice(10), null, 2));
  }
}, 30000);
