const { Pool } = require('pg');
const config = require('./config');

const pool = new Pool(config.postgres);

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_messages (
      id uuid PRIMARY KEY,
      payload jsonb NOT NULL,
      created_at timestamptz NOT NULL DEFAULT NOW()
    )
  `);
}

async function insertMessage(id, payload) {
  const query = `INSERT INTO processed_messages(id, payload) VALUES($1, $2) ON CONFLICT DO NOTHING`;
  await pool.query(query, [id, payload]);
}

module.exports = {
  pool,
  ensureSchema,
  insertMessage
};
