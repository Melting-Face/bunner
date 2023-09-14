import fs from 'fs/promises';
import {
  Readable,
  Writable,
} from 'stream';

import {
  beforeAll,
  expect,
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

  const jsonArray = [];
  const textArray = data.split(rowSeparator);
  const headers = textArray[0].split(headerSeparator);
  for (const text of textArray.slice(1)) {
    if (!text) {
      continue;
    }

    const texts = preprocessForRow(text).split(columnSeparator);
    const entry: any = {};
    for (const i in texts) {
      entry[headers[i]] = texts[i];
    }
    jsonArray.push(entry);
  }
  return jsonArray;
}

beforeAll(async () => {
  for (const fileName of fileNames) {
    try {
      await fs.access(`${fileName}.csv`);
    } catch (e) {
      const response = await request({
        type: 'buffer',
        url: `https://fenixservices.fao.org/faostat/static/bulkdownloads${fileName}.zip`,
      });
      const directory = await unzipper.Open.buffer(response);
      for (const file of directory.files) {
        const fileBuffer = await file.buffer();
        await fs.writeFile(`${fileName}.csv`, fileBuffer);
      }
    }
    const buffer = await fs.readFile(`${fileName}.csv`);
    bufferWithName[fileName] = buffer;
  }
});

async function csvParser(buffer: Buffer, separator: string = ',', rowSeparator: string = '\r\n') {
  logger.info('csv parsing start');
  const jsonArray = [];
  const csvContent = buffer.toString();
  const textArray = csvContent.split(rowSeparator);
  const headers = textArray[0].split(separator);
  for (const text of textArray.slice(1)) {
    if (!text) {
      continue;
    }
    const items = text.replace(/^["]|["]$/g, '').split(`"${separator}"`);
    const entry: any = {};
    for (const i in items) {
      entry[headers[i]] = items[i];
    }
    jsonArray.push(entry);
  }
  logger.info('csv parsing end');
  return jsonArray;
}

test('csv parsing', async () => {
});
