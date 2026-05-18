# Production-Ready Queue System

This project implements a three-tier RabbitMQ queue architecture with PostgreSQL storage.

## Services
- `producer` on port `3000`: sends a new message every 1 second.
- `main-consumer` on port `3001`: primary processing tier.
- `retry-consumer` on port `3002`: transient retry handling.
- `deadletter-consumer` on port `3003`: logs unprocessable messages and retries indefinitely.

## Setup
1. Build and start the stack:
   ```bash
   docker compose up --build
   ```

2. Visit RabbitMQ management:
   - http://localhost:15672
   - default user: `guest`, password: `guest`

3. Check service status:
   - Producer: http://localhost:3000
   - Primary consumer: http://localhost:3001
   - Retry consumer: http://localhost:3002
   - Dead-letter consumer: http://localhost:3003

## Queues and routing
- `main.queue` receives producer messages.
- Failed messages are retried in the main tier up to 3 attempts.
- After 3 failures, messages move to `retry.queue`.
- Retry queue uses exponential backoff and retries up to 5 attempts.
- Permanent failures move to `deadletter.queue`.
- Dead letter queue logs message details and requeues them every 15 seconds.

## Database schema
The app creates the following table on startup:
- `processed_messages(id uuid PRIMARY KEY, payload jsonb, created_at timestamptz NOT NULL DEFAULT NOW())`
