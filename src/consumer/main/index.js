const { startMainConsumer } = require('./mainConsumer');

startMainConsumer().catch((error) => {
  console.error('Main consumer startup error:', error);
  process.exit(1);
});
