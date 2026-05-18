const config = require('../../config');
const { insertMessage } = require('../../db');

// Calculate exponential backoff delay for the retry queue.
function getRetryDelay(retryCount) {
  return config.rabbitmq.retry.retryBaseDelayMs * Math.pow(2, retryCount - 1);
}

async function routeToRetryDelay(channel, msg, retryCount) {
  const headers = { ...msg.properties.headers, 'x-retry-count': retryCount };
  const ttl = getRetryDelay(retryCount);

  channel.publish(
    config.rabbitmq.exchange,
    config.rabbitmq.routingKeys.retryDelay,
    msg.content,
    {
      persistent: true,
      contentType: msg.properties.contentType,
      headers,
      expiration: ttl.toString()
    }
  );
}

// Forward the message to dead letter queue when retry limit is reached.
async function routeToDeadLetter(channel, msg) {
  const headers = { ...msg.properties.headers, 'x-retry-count': msg.properties.headers['x-retry-count'] || 0 };

  channel.publish(
    config.rabbitmq.exchange,
    config.rabbitmq.routingKeys.deadLetter,
    msg.content,
    {
      persistent: true,
      contentType: msg.properties.contentType,
      headers
    }
  );
}

async function handleRetryMessage(channel, msg) {
  const currentRetryCount = Number(msg.properties.headers['x-retry-count'] || 1);

  try {
    const payload = JSON.parse(msg.content.toString('utf8'));
    if (!payload || !payload.id || !payload.payload) {
      throw new Error('Invalid retry payload');
    }

    await insertMessage(payload.id, payload.payload);
    channel.ack(msg);
    console.log(`Retry consumer processed message ${payload.id}`);
  } catch (error) {
    if (currentRetryCount >= config.rabbitmq.retry.retryMaxAttempts) {
      // Too many retry attempts; move the message to the dead letter queue.
      console.warn(`Retry exceeded ${currentRetryCount} attempts, sending to dead letter`, error.message);
      await routeToDeadLetter(channel, msg);
    } else {
      // Delay and retry the message with exponential backoff.
      console.warn(`Retry attempt ${currentRetryCount} failed, delaying ${getRetryDelay(currentRetryCount)}ms`, error.message);
      await routeToRetryDelay(channel, msg, currentRetryCount + 1);
    }
    channel.ack(msg);
  }
}

module.exports = {
  handleRetryMessage
};
