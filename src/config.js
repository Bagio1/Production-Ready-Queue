const port = Number(process.env.PORT || 3000);

module.exports = {
  server: {
    port,
    healthPath: '/health'
  },
  rabbitmq: {
    url: process.env.RABBITMQ_URL || 'amqp://localhost:5672',
    exchange: 'app.exchange',
    queues: {
      main: 'main.queue',
      retry: 'retry.queue',
      retryDelay: 'retry.delay.queue',
      deadLetter: 'deadletter.queue',
      deadLetterDelay: 'deadletter.delay.queue'
    },
    routingKeys: {
      main: 'main',
      retry: 'retry',
      retryDelay: 'retry.delay',
      deadLetter: 'deadletter',
      deadLetterDelay: 'deadletter.delay'
    },
    retry: {
      mainMaxAttempts: 3,
      retryMaxAttempts: 5,
      retryBaseDelayMs: 10000,
      deadLetterRetryDelayMs: 15000
    }
  },
  postgres: {
    host: process.env.PGHOST || 'localhost',
    port: Number(process.env.PGPORT || 5432),
    user: process.env.PGUSER || 'postgres',
    password: process.env.PGPASSWORD || 'postgres',
    database: process.env.PGDATABASE || 'queue_app'
  }
};
