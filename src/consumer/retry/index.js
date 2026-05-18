const { startRetryConsumer } = require('./retryConsumer');

startRetryConsumer().catch((error) => {
  console.error('Retry consumer startup error:', error);
  process.exit(1);
});
