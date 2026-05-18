// Dead letter consumer: logs unprocessable messages and retries them after a fixed delay.
const { createChannel } = require('../../rabbitmq');
const { createStatusServer } = require('../../statusServer');
const config = require('../../config');
const { ensureLogDirectory, handleDeadLetterMessage } = require('./deadLetterHandler');

async function startDeadLetterConsumer() {
  const { connection, channel } = await createChannel();
  ensureLogDirectory();
  createStatusServer(config.server.port, 'deadletter-consumer');

  // Consume from dead-letter queue and archive messages for offline investigation.
  await channel.consume(config.rabbitmq.queues.deadLetter, async (msg) => {
    if (!msg) return;
    await handleDeadLetterMessage(channel, msg);
  }, { noAck: false });

  process.once('SIGINT', async () => {
    await channel.close();
    await connection.close();
    process.exit(0);
  });

  console.log('Dead letter consumer waiting for messages');
}

module.exports = {
  startDeadLetterConsumer
};
