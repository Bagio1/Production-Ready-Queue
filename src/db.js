const { Pool } = require('pg');
const config = require('./config');

const databaseConfig = {
  host: process.env.PGHOST || process.env.POSTGRES_HOST || config.postgres.host || 'localhost',
  port: Number(process.env.PGPORT || process.env.POSTGRES_PORT || config.postgres.port || 5432),
  user: process.env.PGUSER || process.env.POSTGRES_USER || config.postgres.user || 'postgres',
  password: process.env.PGPASSWORD || process.env.POSTGRES_PASSWORD || config.postgres.password || 'postgres',
  database: process.env.PGDATABASE || process.env.POSTGRES_DB || config.postgres.database || 'queue_system'
};

const adminConfig = {
  ...databaseConfig,
  database: process.env.PGADMIN_DATABASE || 'postgres'
};

const pool = new Pool(databaseConfig);
let shutdownStarted = false;

pool.on('connect', () => {
  console.log(`PostgreSQL connected to ${databaseConfig.host}:${databaseConfig.port}/${databaseConfig.database}`);
});

pool.on('error', (error) => {
  console.error('Unexpected PostgreSQL pool error:', error.stack || error);
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeLimit(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    return 50;
  }
  return Math.min(parsed, 500);
}

function normalizeOffset(value) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    return 0;
  }
  return parsed;
}

function normalizeInsertArgs(content, status = 'success', retryCount = 0) {
  const validStatuses = new Set(['success', 'retry', 'dead_letter']);
  let normalizedContent = content;

  if (!validStatuses.has(status)) {
    normalizedContent = { id: content, payload: status };
    status = 'success';
  }

  return {
    content: typeof normalizedContent === 'string' ? normalizedContent : JSON.stringify(normalizedContent),
    status,
    retryCount: Number.isInteger(Number(retryCount)) ? Number(retryCount) : 0
  };
}

async function query(text, params = [], options = {}) {
  const {
    retries = 3,
    retryDelayMs = 250,
    logQuery = true
  } = options;

  let lastError;

  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const startedAt = Date.now();

    try {
      const result = await pool.query(text, params);
      const durationMs = Date.now() - startedAt;

      if (logQuery) {
        console.log(`PostgreSQL query completed in ${durationMs}ms`);
      }

      return result;
    } catch (error) {
      lastError = error;
      const durationMs = Date.now() - startedAt;
      console.error(`PostgreSQL query failed in ${durationMs}ms on attempt ${attempt}/${retries}:`, error.stack || error);

      if (attempt < retries) {
        await sleep(retryDelayMs * attempt);
      }
    }
  }

  throw lastError;
}

async function createDatabaseIfNotExists() {
  const adminPool = new Pool(adminConfig);
  const databaseName = databaseConfig.database;

  try {
    const result = await adminPool.query('SELECT 1 FROM pg_database WHERE datname = $1', [databaseName]);

    if (result.rowCount === 0) {
      const quotedName = databaseName.replace(/"/g, '""');
      await adminPool.query(`CREATE DATABASE "${quotedName}"`);
      console.log(`PostgreSQL database "${databaseName}" created`);
    } else {
      console.log(`PostgreSQL database "${databaseName}" already exists`);
    }
  } catch (error) {
    console.error('PostgreSQL database initialization error:', error.stack || error);
    throw error;
  } finally {
    await adminPool.end();
  }
}

async function ensureSchema() {
  await createDatabaseIfNotExists();

  await query(`
    CREATE TABLE IF NOT EXISTS messages (
      id SERIAL PRIMARY KEY,
      content TEXT,
      retry_count INTEGER NOT NULL DEFAULT 0,
      status VARCHAR(20) NOT NULL CHECK (status IN ('success', 'retry', 'dead_letter')),
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      processed_at TIMESTAMP NULL
    )
  `);
}

async function initializeDatabase() {
  await ensureSchema();
  console.log('PostgreSQL database initialized successfully');
}

async function insertMessage(content, status = 'success', retryCount = 0) {
  const message = normalizeInsertArgs(content, status, retryCount);
  const result = await query(
    `
      INSERT INTO messages (content, status, retry_count, processed_at)
      VALUES ($1, $2, $3, CASE WHEN $2 = 'success' THEN CURRENT_TIMESTAMP ELSE NULL END)
      RETURNING *
    `,
    [message.content, message.status, message.retryCount]
  );

  return result.rows[0];
}

async function updateMessageStatus(id, status, retryCount = 0) {
  const result = await query(
    `
      UPDATE messages
      SET status = $2,
          retry_count = $3,
          processed_at = CASE WHEN $2 = 'success' THEN CURRENT_TIMESTAMP ELSE processed_at END
      WHERE id = $1
      RETURNING *
    `,
    [id, status, retryCount]
  );

  return result.rows[0] || null;
}

async function getFailedMessages(limit = 50, offset = 0) {
  const result = await query(
    `
      SELECT *
      FROM messages
      WHERE processed_at IS NULL
        AND status IN ('retry', 'dead_letter')
      ORDER BY created_at ASC, id ASC
      LIMIT $1 OFFSET $2
    `,
    [normalizeLimit(limit), normalizeOffset(offset)]
  );

  return result.rows;
}

async function getMessageById(id) {
  const result = await query('SELECT * FROM messages WHERE id = $1', [id]);
  return result.rows[0] || null;
}

async function deleteMessage(id) {
  const result = await query('DELETE FROM messages WHERE id = $1 RETURNING *', [id]);
  return result.rows[0] || null;
}

async function getAllMessages(limit = 50, offset = 0) {
  const result = await query(
    `
      SELECT *
      FROM messages
      ORDER BY created_at DESC, id DESC
      LIMIT $1 OFFSET $2
    `,
    [normalizeLimit(limit), normalizeOffset(offset)]
  );

  return result.rows;
}

async function closePool() {
  if (shutdownStarted) {
    return;
  }

  shutdownStarted = true;

  try {
    await pool.end();
    console.log('PostgreSQL connection pool closed');
  } catch (error) {
    console.error('Error closing PostgreSQL connection pool:', error.stack || error);
  }
}

process.once('SIGINT', closePool);
process.once('SIGTERM', closePool);

module.exports = {
  pool,
  query,
  initializeDatabase,
  ensureSchema,
  insertMessage,
  updateMessageStatus,
  getFailedMessages,
  getMessageById,
  deleteMessage,
  getAllMessages,
  closePool
};
