import { test } from 'bun:test';

import {
  kafka,
  logger,
} from '../lib/utils';

test('consume', async () => {
  const consumer = kafka.consumer({ groupId: 'my-group' });
  await consumer.run({
    eachMessage: async ({
      topic,
      partition,
      message,
      heartbeat,
      pause,
    }) => {
      logger.info({
        key: message.key?.toString(),
        value: message.value?.toString(),
        headers: message.headers,
      });
    },
  });
});
