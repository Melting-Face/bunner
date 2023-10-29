import fs from 'fs/promises';
import { Duplex } from 'stream';
import { pipeline } from 'stream/promises';

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
