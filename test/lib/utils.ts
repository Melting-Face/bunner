import { insert } from 'sql-bricks';
import {
  createLogger,
  format,
  transports,
} from 'winston';

const logger = createLogger({
  level: 'info',
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.colorize(),
    format.ms(),
    format.printf(({
      level,
      message,
      timestamp,
      ms,
    }) => `${timestamp} ${level}: ${message} ${ms}`),
  ),
  transports: [new transports.Console()],
});

interface Args {
  url: string;
  body?: string | object;
  form?: string | object;
  type?: string;
  method?: string;
  headers?: object;
}

interface ContentType {
  'Content-Type'?: string;
}

export default async function request(args: string | Args): Promise<any> {
  let headerOptions;
  let bodyOptions;
  let formOptions;
  let response: Response;
  let url: string = '';
  let method: string = 'GET';
  let type: string = 'text';
  let contentType: ContentType = {};

  try {
    if (typeof args === 'string') {
      url = args;
      response = await fetch(args);
    } else {
      ({
        url,
        method = 'GET',
        type = 'text',
        headers: headerOptions,
        body: bodyOptions,
        form: formOptions,
      } = args);
      const requestObject: any = {
        method,
        verbose: true,
      };

      if (bodyOptions) {
        switch (typeof bodyOptions) {
          case 'object':
            bodyOptions = JSON.stringify(bodyOptions);
            break;
          case 'string':
            break;
          default:
            throw new Error('Check to body type(string, object)');
        }
        const body = bodyOptions;
        contentType = { 'Content-Type': 'application/json' };
        requestObject.body = body;
      } else if (formOptions) {
        switch (typeof formOptions) {
          case 'object':
            formOptions = new URLSearchParams(Object.entries(formOptions)).toString();
            break;
          case 'string':
            break;
          default:
            throw new Error('Check to form type(string, object)');
        }
        const form = formOptions;
        contentType = { 'Content-Type': 'application/x-www-form-urlencoded' };
        requestObject.body = form;
      }

      if (Object.keys(contentType).length || headerOptions) {
        headerOptions = Object.entries({
          ...contentType,
          ...headerOptions,
        }).map(([key, value]) => [key, String(value)]);
        const headers = headerOptions;
        requestObject.headers = headers;
      }
      response = await fetch(url, requestObject);
    }

    if (!response.ok) {
      throw new Error(`response error ${response.status} ${response.statusText}`);
    }

    switch (type) {
      case 'blob':
        return response.blob();
      case 'buffer':
        return Buffer.from(await response.arrayBuffer());
      case 'text':
        return response.text();
      case 'json':
        return response.json();
      default:
        return response;
    }
  } catch (e) {
    logger.error(`fetch error(url: ${url}) :${e}`);
    throw new Error(`error(url: '${url}' ): ${e}`);
  }
}

async function delay(num: number) {
  await new Promise((resolve) => setTimeout(resolve, num));
}

class Ksql {
  source: string;

  query: string = '';

  constructor(source: string) {
    this.source = source;
  }

  async #initialize(entry: object) {
    const fields = [];
    for (const key in entry) {
      if (key !== 'hash') {
        fields.push(`${key} VARCHAR`);
      }
    }

    this.query = `
    CREATE TABLE IF NOT EXISTS ${this.source.toUpperCase()} (
        hash VARCHAR PRIMARY KEY,
        ${fields.join(',\n')}
    ) WITH (
        kafka_topic='${this.source}',
        value_format='json',
        partitions=1
    );`;
    await this.#push();
  }

  async insertMany(entries: Array<object>) {
    await this.#initialize(entries[0]);
    const queries = [];
    for (const entry of entries) {
      const query = `${insert(this.source.toUpperCase(), entry).toString()};`;
      queries.push(query);
    }

    this.query = queries.join('\n');
    await this.#push();
  }

  async #push() {
    logger.info(this.query);
    await request({
      url: 'http://localhost:8088/ksql',
      method: 'POST',
      headers: { Accept: 'application/vnd.ksql.v1+json' },
      body: {
        ksql: this.query,
        streamsProperties: {},
      },
    });
  }
}

export {
  delay,
  Ksql,
  logger,
  request,
};
