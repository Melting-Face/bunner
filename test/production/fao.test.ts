import fs from 'fs/promises';
import {
  Readable,
  Writable,
} from 'stream';
import { pipeline } from 'stream/promises';

import {
  beforeAll,
  expect,
  test,
} from 'bun:test';
import pl from 'nodejs-polars';
import {
  ParquetSchema,
  ParquetTransformer,
  ParquetWriter,
} from 'parquetjs';

import { logger } from '../lib/utils';

class JSONStream extends Readable {
  array: Array<object>;

  index: number;

  constructor(array: Array<object>) {
    super({ objectMode: true });
    this.array = array;
    this.index = 0;
  }

  _read() {
    if (this.index < this.array.length) {
      const data = this.array[this.index];
      this.push(data);
      this.index += 1;
    } else {
      this.push(null); // No more data
    }
  }
}

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
    const buffer = await fs.readFile(`${fileName}.csv`);
    bufferWithName[fileName] = buffer;
  }
});

test('nodejs-polars', async () => {
  for (const fileName of fileNames) {
    console.time(`${fileName}`);
    const df = pl.readCSV(bufferWithName[fileName]);
    console.timeEnd(`${fileName}`);
    const buffer = df.writeParquet({ compression: 'uncompressed' });
    await fs.writeFile(`${fileName}-2.parquet`, buffer);
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

test('parquetjs-pipeline', async () => {
  for (const fileName of fileNames) {
    logger.info(fileName);
    const buffer = bufferWithName[fileName];
    const items = await csvParser(buffer);
    const schema = await columnToSchema(items);
    const chunks: any = [];
    const writableStream = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(chunk);
        callback();
      },
    });
    console.time(`${fileName}`);
    await pipeline(
      new JSONStream(items),
      new ParquetTransformer(schema),
      writableStream,
    );
    console.timeEnd(`${fileName}`);
    const writeBuffer = Buffer.concat(chunks);
    logger.info(`Byte length: ${Buffer.byteLength(writeBuffer)}`);
    expect(Buffer.byteLength(writeBuffer)).toBeTruthy();
    await fs.writeFile(`${fileName}.parquet`, writeBuffer);
    // logger.info('End to write file');
  }
}, 1000000);

test('parquetjs-for_statement', async () => {
  for (const fileName of fileNames) {
    logger.info(fileName);
    const buffer = bufferWithName[fileName];
    const items = await csvParser(buffer);
    const schema = await columnToSchema(items);
    const chunks: any = [];
    const writableStream = new Writable({
      write(chunk, _encoding, callback) {
        chunks.push(chunk);
        callback();
      },
    });
    const writer = await ParquetWriter.openStream(schema, writableStream);
    console.time(`${fileName}`);
    for (const item of items) {
      await writer.appendRow(item);
    }
    console.timeEnd(`${fileName}`);
    const writeBuffer = Buffer.concat(chunks);
    logger.info(`Byte length: ${Buffer.byteLength(writeBuffer)}`);
    expect(Buffer.byteLength(writeBuffer)).toBeTruthy();
    await fs.writeFile(`${fileName}.parquet`, writeBuffer);
    // logger.info('End to write file');
  }
}, 1000000);
