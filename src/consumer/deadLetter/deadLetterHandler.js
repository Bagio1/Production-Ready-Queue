// Dead-letter handler: archives bad messages to disk and requeues them after a fixed delay.
const fs = require('fs');
const path = require('path');
const config = require('../../config');

const LOG_FILE = path.join(__dirname, '..', '..', 'logs', 'deadletter.log');

function ensureLogDirectory() {
  const logDir = path.dirname(LOG_FILE);
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
}

function appendDeadLetterEntry(payload, headers, errorMessage) {
  const entry = {
    timestamp: new Date().toISOString(),
    headers,
    errorMessage,
    payload
  };

  fs.appendFileSync(LOG_FILE, JSON.stringify(entry) + '\n');
}

async function routeToDeadLetterDelay(channel, msg) {
  const headers = { ...msg.properties.headers };

  channel.publish(
    config.rabbitmq.exchange,
    config.rabbitmq.routingKeys.deadLetterDelay,
    msg.content,
    {
      persistent: true,
      contentType: msg.properties.contentType,
      headers,
      expiration: config.rabbitmq.retry.deadLetterRetryDelayMs.toString()
    }
  );
}

async function handleDeadLetterMessage(channel, msg) {
  const message = JSON.parse(msg.content.toString('utf8'));
  const headers = msg.properties.headers || {};

  appendDeadLetterEntry(message, headers, 'Dead-letter retry logging');
  console.warn(`Dead letter consumer archived message ${message.id || '<unknown>'}`);

  await routeToDeadLetterDelay(channel, msg);
  channel.ack(msg);
}

module.exports = {
  ensureLogDirectory,
  handleDeadLetterMessage
};
