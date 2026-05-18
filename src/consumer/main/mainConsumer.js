const { createChannel } = require('../../rabbitmq');
const { createStatusServer } = require('../../statusServer');
const { ensureSchema } = require('../../db');
const config = require('../../config');
const { handleMainMessage } = require('./mainHandler');

async function startMainConsumer() {
  const { connection, channel } = await createChannel();
  await ensureSchema();
  createStatusServer(config.server.port, 'main-consumer');

  await channel.consume(config.rabbitmq.queues.main, async (msg) => {
    if (!msg) return;
    await handleMainMessage(channel, msg);
  }, { noAck: false });

  process.once('SIGINT', async () => {
    await channel.close();
    await connection.close();
    process.exit(0);
  });

  console.log('Main consumer waiting for messages');
}

module.exports = {
  startMainConsumer
};
