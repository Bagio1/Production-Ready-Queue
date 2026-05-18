const { createChannel } = require('../../rabbitmq');
const { createStatusServer } = require('../../statusServer');
const { ensureSchema } = require('../../db');
const config = require('../../config');
const { handleRetryMessage } = require('./retryHandler');

async function startRetryConsumer() {
  const { connection, channel } = await createChannel();
  await ensureSchema();
  createStatusServer(config.server.port, 'retry-consumer');

  await channel.consume(config.rabbitmq.queues.retry, async (msg) => {
    if (!msg) return;
    await handleRetryMessage(channel, msg);
  }, { noAck: false });

  process.once('SIGINT', async () => {
    await channel.close();
    await connection.close();
    process.exit(0);
  });

  console.log('Retry consumer waiting for messages');
}

module.exports = {
  startRetryConsumer
};
