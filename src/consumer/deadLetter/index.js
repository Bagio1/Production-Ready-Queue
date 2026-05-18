const { startDeadLetterConsumer } = require('./deadLetterConsumer');

startDeadLetterConsumer().catch((error) => {
  console.error('Dead letter consumer startup error:', error);
  process.exit(1);
});
