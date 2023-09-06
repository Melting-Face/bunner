import {
  expect,
  test,
} from 'bun:test';
import pl from 'nodejs-polars';
import unzipper from 'unzipper';

import request from '../lib/request';
import { logger } from '../lib/utils';

const bufferWithFileName: any = {};
const fileNames = [
  'Prices_E_All_Data_(Normalized)',
  'Production_Crops_Livestock_E_All_Data_(Normalized)',
];

test('file download', async () => {
  const downloadUrl = 'https://fenixservices.fao.org/faostat/static/bulkdownloads';

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
      expect(name).toBeTruthy();
      if (`${fileName}.csv` === name) {
        logger.info(name);
        const buffer: Buffer = await file.buffer();
        bufferWithFileName[fileName] = buffer;
      }
    }

    expect(fileName).toBeTruthy();
  }
}, 100000);

test('csv with polars', async () => {
  for (const fileName of fileNames) {
    const df = pl.readCSV(bufferWithFileName[fileName]);
    df.writeParquet(`${fileName}-1.parquet`);
  }
});
