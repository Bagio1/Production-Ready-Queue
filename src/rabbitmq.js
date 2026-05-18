const amqp = require('amqplib');
const config = require('./config');

async function createChannel() {
  const connection = await amqp.connect(config.rabbitmq.url);
  const channel = await connection.createChannel();

  await channel.assertExchange(config.rabbitmq.exchange, 'direct', { durable: true });

  await channel.assertQueue(config.rabbitmq.queues.main, { durable: true });
  await channel.assertQueue(config.rabbitmq.queues.retry, { durable: true });
  await channel.assertQueue(config.rabbitmq.queues.retryDelay, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': config.rabbitmq.exchange,
      'x-dead-letter-routing-key': config.rabbitmq.routingKeys.retry
    }
  });
  await channel.assertQueue(config.rabbitmq.queues.deadLetter, { durable: true });
  await channel.assertQueue(config.rabbitmq.queues.deadLetterDelay, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': config.rabbitmq.exchange,
      'x-dead-letter-routing-key': config.rabbitmq.routingKeys.deadLetter
    }
  });

  await channel.bindQueue(config.rabbitmq.queues.main, config.rabbitmq.exchange, config.rabbitmq.routingKeys.main);
  await channel.bindQueue(config.rabbitmq.queues.retry, config.rabbitmq.exchange, config.rabbitmq.routingKeys.retry);
  await channel.bindQueue(config.rabbitmq.queues.retryDelay, config.rabbitmq.exchange, config.rabbitmq.routingKeys.retryDelay);
  await channel.bindQueue(config.rabbitmq.queues.deadLetter, config.rabbitmq.exchange, config.rabbitmq.routingKeys.deadLetter);
  await channel.bindQueue(config.rabbitmq.queues.deadLetterDelay, config.rabbitmq.exchange, config.rabbitmq.routingKeys.deadLetterDelay);

  return { connection, channel };
}

module.exports = {
  createChannel
};
