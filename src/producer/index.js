const { startProducer } = require('./producer');

startProducer().catch((error) => {
  console.error('Producer startup error:', error);
  process.exit(1);
});
