import { Kafka } from 'kafkajs';
import {
  createLogger,
  format,
  transports,
} from 'winston';

async function delay(num: number) {
  await new Promise((resolve) => setTimeout(resolve, num));
}

function getProducer() {
  const kafka = new Kafka({ brokers: ['127.0.0.1:29092'] });
  const producer = kafka.producer();
  return producer;
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
  getProducer,
  logger,
};
