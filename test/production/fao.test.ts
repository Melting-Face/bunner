import fs from 'fs/promises';
import { Writable } from 'stream';

import {
  expect,
  test,
} from 'bun:test';
import {
  ParquetSchema,
  ParquetWriter,
} from 'parquetjs';

// import unzipper from 'unzipper';
//
// import request from '../lib/request';
import { logger } from '../lib/utils';

const fileNames = [
  'Prices_E_All_Data_(Normalized)',
  'Production_Crops_Livestock_E_All_Data_(Normalized)',
];

// beforeAll(async () => {
//   const downloadUrl = 'https://fenixservices.fao.org/faostat/static/bulkdownloads';
//
//   for (const fileName of fileNames) {
//     const url = `${downloadUrl}/${fileName}.zip`;
//     logger.info(`Fetching ... ${url}`);
//     const response = await request({
//       url,
//       type: 'buffer',
//     });
//
//     const directory = await unzipper.Open.buffer(response);
//     for (const file of directory.files) {
//       const name = file.path;
//       if (`${fileName}.csv` === name) {
//         logger.info(name);
//         const buffer: Buffer = await file.buffer();
//         await fs.writeFile(`${fileName}.csv`, buffer);
//       }
//     }
//   }
// });

// test('csv with polars', async () => {
//   for (const fileName of fileNames) {
//     const df = pl.readCSV(bufferWithFileName[fileName]);
//     const buffer = df.writeParquet({ compression: 'uncompressed' });
//     expect(buffer).toBeTruthy();
//     logger.info(buffer);
//   }
// });
//
async function csvParser(buffer: Buffer, separator: string = ',', rowSeparator: string = '\r\n') {
  logger.info('csv parsing start');
  const jsonArray = [];
  const csvContent = buffer.toString();
  const textArray = csvContent.split(rowSeparator);
  const headers = textArray[0].split(separator);
  for (const text of textArray.slice(1)) {
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

async function columnToSchema(items: any) {
  logger.info('column parsing start');
  const schema: any = {};

  for (const item of items) {
    for (const column in item) {
      schema[column] = { type: 'UTF8' };
    }
  }

  logger.info('column parsing end');
  return new ParquetSchema(schema);
}

test('csv parse and convert to json - 1', async () => {
  for (const fileName of fileNames) {
    logger.info(fileName);
    const buffer = await fs.readFile(`${fileName}.csv`);
    const items = await csvParser(buffer);
    expect(items).toBeTruthy();
    const schema = await columnToSchema(items);
    // logger.info(JSON.stringify(items.slice(0, 10), null, 2));
    const parquetChunks: any = [];
    const writableStream = new Writable({
      write(chunk, _encoding, callback) {
        parquetChunks.push(chunk);
        callback();
      },
    });
    const writer = await ParquetWriter.openStream(schema, writableStream);
    writer.setRowGroupSize(2048);
    logger.info('Start to write row');
    let count = 1;
    await Promise.all(items.map(async (item) => {
      logger.info(`Write row ${count}`);
      await writer.appendRow(item);
      count += 1;
    }));
    writer.close();
    logger.info('End to write row');
    await fs.writeFile(`${fileName}.parquet`, Buffer.concat(parquetChunks));
  }
}, 1000000);

// async function csvParser2(buffer: Buffer, separator: string = ',', rowSeparator: string = '\r\n') {
//   const csvContent = buffer.toString();
//   const textArray = csvContent.split(rowSeparator);
//   const headers = textArray[0].split(separator);
//   const result = await Promise.all(
//     textArray.slice(1).map((text) => {
//       const entry: any = {};
//       text.split(separator).forEach((value, index) => {
//         entry[headers[index]] = value;
//       });
//       return entry;
//     }),
//   );
//   logger.info(JSON.stringify(result, null, 2));
// }
//
// test('csv parse and convert to json - 2', async () => {
//   for (const fileName of fileNames) {
//     const buffer = bufferWithFileName[fileName];
//     await csvParser2(buffer);
//   }
// });
