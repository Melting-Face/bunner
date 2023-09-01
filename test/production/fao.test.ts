import fs from 'fs/promises';

import {
  expect,
  test,
} from 'bun:test';
import unzipper from 'unzipper';

import request from '../lib/request';
import { logger } from '../lib/utils';

test('file download', async () => {
  const downloadUrl = 'https://fenixservices.fao.org/faostat/static/bulkdownloads';
  const fileNames = [
    'Prices_E_All_Data_(Normalized)',
    'Production_Crops_Livestock_E_All_Data_(Normalized)',
  ];

  for (const fileName of fileNames) {
    const url = `${downloadUrl}/${fileName}.zip`;
    logger.info(`Fetching ... ${url}`);
    const response = await request({
      url,
      type: 'buffer',
    });

    const directory = await unzipper.Open.buffer(response);
    for (const file of directory.files) {
      const name = file.path;
      logger.info(name);
      expect(name).toBeTruthy();
      if (`${fileName}.csv` === name) {
        const buffer: Buffer = await file.buffer();
        logger.info(name);
        await fs.writeFile(`${fileName}.csv`, buffer);
      }
    }

    expect(fileName).toBeTruthy();
  }
}, 100000);
