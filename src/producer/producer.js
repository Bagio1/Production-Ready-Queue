// Producer module: creates periodic messages and publishes them to the main queue.
const { v4: uuidv4 } = require('uuid');
const { createChannel } = require('../rabbitmq');
const { createStatusServer } = require('../statusServer');
const config = require('../config');

const PRODUCER_INTERVAL_MS = 1000;
let messageCount = 0;

function createMessage() {
  return {
    id: uuidv4(),
    payload: {
      createdAt: new Date().toISOString(),
      body: `Payload message ${messageCount + 1}`
    }
  };
}

async function publishMessage(channel, message) {
  // Convert the message to JSON and set RabbitMQ options for durability.
  const content = Buffer.from(JSON.stringify(message));
  const options = {
    contentType: 'application/json',
    persistent: true,
    headers: { 'x-retry-count': 0 }
  };

  return channel.publish(
    config.rabbitmq.exchange,
    config.rabbitmq.routingKeys.main,
    content,
    options
  );
}

async function startProducer() {
  const { connection, channel } = await createChannel();
  createStatusServer(config.server.port, 'producer');

  process.once('SIGINT', async () => {
    await channel.close();
    await connection.close();
    process.exit(0);
  });

  setInterval(async () => {
    const message = createMessage();
    const published = await publishMessage(channel, message);

    if (published) {
      messageCount += 1;
      console.log(`Produced message ${message.id} [${messageCount}]`);
    } else {
      console.warn('Producer failed to publish message, channel is busy');
    }
  }, PRODUCER_INTERVAL_MS);
}

module.exports = {
  startProducer
};
