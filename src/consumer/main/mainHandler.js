const config = require('../../config');
const { insertMessage } = require('../../db');

// Sends a message back into a queue with an updated retry count header.
function routeMessage(channel, routingKey, msg, retryCount) {
  const headers = { ...msg.properties.headers, 'x-retry-count': retryCount };
  channel.publish(
    config.rabbitmq.exchange,
    routingKey,
    msg.content,
    {
      persistent: true,
      contentType: msg.properties.contentType,
      headers
    }
  );
}

// Primary consumer logic for the main queue.
async function handleMainMessage(channel, msg) {
  const currentRetryCount = Number(msg.properties.headers['x-retry-count'] || 0);
  const nextRetryCount = currentRetryCount + 1;

  try {
    const payload = JSON.parse(msg.content.toString('utf8'));
    if (!payload || !payload.id || !payload.payload) {
      throw new Error('Invalid message payload');
    }

    // Persist the message payload in PostgreSQL before acknowledging.
    await insertMessage(payload.id, payload.payload);
    channel.ack(msg);
    console.log(`Main consumer processed message ${payload.id}`);
  } catch (error) {
    if (nextRetryCount >= config.rabbitmq.retry.mainMaxAttempts) {
      console.warn(`Main failure after ${nextRetryCount} attempts, forwarding to retry queue`, error.message);
      await routeMessage(channel, config.rabbitmq.routingKeys.retry, msg, 1);
    } else {
      console.warn(`Main attempt ${nextRetryCount} failed, retrying on main queue`, error.message);
      await routeMessage(channel, config.rabbitmq.routingKeys.main, msg, nextRetryCount);
    }
    channel.ack(msg);
  }
}

module.exports = {
  handleMainMessage
};
