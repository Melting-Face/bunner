import { Kafka } from 'kafkajs';
import {
  createLogger,
  format,
  transports,
} from 'winston';

const kafka = new Kafka({ brokers: ['localhost:29092'] });

async function delay(num: number) {
  await new Promise((resolve) => setTimeout(resolve, num));
}

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

export {
  delay,
  kafka,
  logger,
};
