# Production-Ready Queue System

A 3-tier RabbitMQ queue that never silently drops a message.

In a simple queue, one message that keeps failing can be retried forever and block everything behind it. This project separates failures into tiers: fast retries first, slower retries with exponential backoff next, and a dead-letter queue last, where failed messages are parked, logged and retried later instead of being lost. Every message's status is tracked in PostgreSQL.

**Stack:** Node.js · RabbitMQ (amqplib) · PostgreSQL · Docker Compose

## How it works

```
Producer (1 msg/sec)
      │
      ▼
┌──────────────┐  fails 3×   ┌──────────────┐  fails 5×   ┌──────────────────┐
│  main.queue  │ ──────────► │ retry.queue  │ ──────────► │ deadletter.queue │
│  3 attempts  │             │  backoff     │             │  retry every 15s │
└──────────────┘             └──────────────┘             └──────────────────┘
        │                           │                              │
        └───────────────────────────┴──────────────────────────────┘
                                    ▼
                    PostgreSQL: status + retry_count per message
```

| Tier | Queue | Attempts | Delay |
| --- | --- | --- | --- |
| 1. Main | `main.queue` | up to 3 | immediate |
| 2. Retry | `retry.queue` | up to 5 | exponential: 10s, 20s, 40s, 80s, 160s |
| 3. Dead letter | `deadletter.queue` | unlimited | every 15s, with full logging |

Delays are implemented with delay queues (`retry.delay.queue`, `deadletter.delay.queue`) on a single exchange (`app.exchange`). All values are in `src/config.js`.

## Services

| Service | Port | Role |
| --- | --- | --- |
| `producer` | 3000 | publishes one message per second |
| `main-consumer` | 3001 | primary processing tier |
| `retry-consumer` | 3002 | backoff retries |
| `deadletter-consumer` | 3003 | parks, logs and re-queues permanent failures |
| `rabbitmq` | 5672 / 15672 | broker + management UI |
| `postgres` | 5432 | message status storage |

Each Node service exposes `GET /health` for health checks.

## Run it

```bash
docker compose up --build
```

Then open:

- RabbitMQ management: http://localhost:15672 (local default login `guest` / `guest`)
- Service status: http://localhost:3000/health … http://localhost:3003/health

Stop everything with `docker compose down`.

## Database

The `messages` table is created on startup:

| Column | Type | Notes |
| --- | --- | --- |
| `id` | `SERIAL` | primary key |
| `content` | `TEXT` | message payload |
| `retry_count` | `INTEGER` | attempts so far |
| `status` | `VARCHAR(20)` | `success`, `retry` or `dead_letter` |
| `created_at` | `TIMESTAMP` | when it was stored |
| `processed_at` | `TIMESTAMP` | when processing finished |

Database queries themselves retry with a short backoff, so a brief PostgreSQL hiccup doesn't lose a status update.

## Tests

With the stack running:

```bash
npm install
npm test
```

`test.js` is an integration suite that checks the producer, the three consumers, RabbitMQ and PostgreSQL together.

## Project structure

```
src/
├── config.js            # queues, routing keys, retry limits, delays
├── rabbitmq.js          # connection, exchange and queue setup
├── db.js                # PostgreSQL pool, schema, queries
├── statusServer.js      # /health endpoint for each service
├── producer/
└── consumer/
    ├── main/
    ├── retry/
    └── deadLetter/
```

## Author

Mohamed Fouhamy · [bagio.dev](https://bagio.dev) · [LinkedIn](https://www.linkedin.com/in/moxa-f/)
