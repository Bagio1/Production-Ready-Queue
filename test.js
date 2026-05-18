#!/usr/bin/env node

/**
 * Integration Test Suite for Production-Ready Queue System
 * Tests all components: Producer, Consumers, RabbitMQ, PostgreSQL
 */

const http = require('http');
const amqp = require('amqplib');
const { Client } = require('pg');
const { v4: uuidv4 } = require('uuid');
const {
  initializeDatabase,
  insertMessage,
  updateMessageStatus,
  getFailedMessages,
  getMessageById,
  deleteMessage,
  getAllMessages,
  closePool,
} = require('./src/db');

const config = {
  rabbitmq: {
    host: process.env.RABBITMQ_HOST || 'localhost',
    port: process.env.RABBITMQ_PORT || 5672,
  },
  postgres: {
    host: process.env.POSTGRES_HOST || process.env.PGHOST || 'localhost',
    port: Number(process.env.POSTGRES_PORT || process.env.PGPORT || 5432),
    database: process.env.POSTGRES_DB || process.env.PGDATABASE || 'queue_system',
    user: process.env.POSTGRES_USER || process.env.PGUSER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || process.env.PGPASSWORD || 'postgres',
  },
  services: {
    producer: 'http://localhost:3000/health',
    mainConsumer: 'http://localhost:3001/health',
    retryConsumer: 'http://localhost:3002/health',
    deadletterConsumer: 'http://localhost:3003/health',
  },
};

let testsPassed = 0;
let testsFailed = 0;
let publishedMessageId = null;

/**
 * Helper function to make HTTP requests
 */
function makeRequest(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let data = '';
      res.on('data', (chunk) => (data += chunk));
      res.on('end', () => {
        if (res.statusCode === 200) {
          try {
            resolve(JSON.parse(data));
          } catch {
            resolve(data);
          }
        } else {
          reject(new Error(`HTTP ${res.statusCode}`));
        }
      });
    }).on('error', reject);
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForCondition(check, timeoutMs = 10000, intervalMs = 500) {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    const result = await check();
    if (result) {
      return result;
    }

    await sleep(intervalMs);
  }

  return null;
}

/**
 * Test 1: Health Check - All Services
 */
async function testHealthChecks() {
  console.log('\n📋 Test 1: Health Checks - All Services');
  try {
    const services = Object.entries(config.services);
    let allHealthy = true;

    for (const [name, url] of services) {
      try {
        const response = await makeRequest(url);
        console.log(`  ✅ ${name}: ${response.status}`);
      } catch (error) {
        console.log(`  ❌ ${name}: ${error.message}`);
        allHealthy = false;
      }
    }

    if (allHealthy) {
      testsPassed++;
      console.log('  Result: PASSED ✓');
    } else {
      testsFailed++;
      console.log('  Result: FAILED ✗');
    }
  } catch (error) {
    testsFailed++;
    console.log(`  ❌ Error: ${error.message}`);
  }
}

/**
 * Test 2: RabbitMQ Connection & Queue Setup
 */
async function testRabbitMQConnection() {
  console.log('\n📋 Test 2: RabbitMQ Connection & Queue Setup');
  try {
    const url = `amqp://${config.rabbitmq.host}:${config.rabbitmq.port}`;
    const connection = await amqp.connect(url);
    const channel = await connection.createChannel();

    // Declare queues
    await channel.assertQueue('main.queue', { durable: true });
    await channel.assertQueue('retry.queue', { durable: true });
    await channel.assertQueue('deadletter.queue', { durable: true });

    console.log('  ✅ Connected to RabbitMQ');
    console.log('  ✅ Declared main.queue');
    console.log('  ✅ Declared retry.queue');
    console.log('  ✅ Declared deadletter.queue');

    await channel.close();
    await connection.close();

    testsPassed++;
    console.log('  Result: PASSED ✓');
  } catch (error) {
    testsFailed++;
    console.log(`  ❌ Error: ${error.message}`);
  }
}

/**
 * Test 3: PostgreSQL Connection & Schema
 */
async function testPostgresConnection() {
  console.log('\n📋 Test 3: PostgreSQL Connection & Schema');
  try {
    await initializeDatabase();

    const client = new Client(config.postgres);
    await client.connect();

    const result = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_name='messages';"
    );

    if (result.rows.length > 0) {
      console.log('  ✅ Connected to PostgreSQL');
      console.log('  ✅ messages table exists');

      const countResult = await client.query('SELECT COUNT(*) FROM messages;');
      console.log(`  ℹ️  Total messages in DB: ${countResult.rows[0].count}`);

      testsPassed++;
      console.log('  Result: PASSED ✓');
    } else {
      throw new Error('processed_messages table not found');
    }

    await client.end();
  } catch (error) {
    testsFailed++;
    console.log(`  ❌ Error: ${error.message}`);
  }
}

/**
 * Test 4: Producer - Send Test Message to Main Queue
 */
async function testProducerMessage() {
  console.log('\n📋 Test 4: Producer - Send Test Message to Main Queue');
  try {
    const url = `amqp://${config.rabbitmq.host}:${config.rabbitmq.port}`;
    const connection = await amqp.connect(url);
    const channel = await connection.createChannel();

    const testMessage = {
      id: uuidv4(),
      type: 'test',
      timestamp: new Date().toISOString(),
      payload: { test: true, value: Math.random() },
    };

    await channel.assertQueue('main.queue', { durable: true });
    const sent = channel.sendToQueue(
      'main.queue',
      Buffer.from(JSON.stringify(testMessage)),
      {
        contentType: 'application/json',
        persistent: true,
        headers: { 'x-retry-count': 0 },
      }
    );

    if (sent) {
      publishedMessageId = testMessage.id;
      console.log(`  ✅ Test message sent to main.queue`);
      console.log(`  ℹ️  Message ID: ${testMessage.id}`);
      testsPassed++;
      console.log('  Result: PASSED ✓');
    } else {
      throw new Error('Failed to send message');
    }

    await channel.close();
    await connection.close();
  } catch (error) {
    testsFailed++;
    console.log(`  ❌ Error: ${error.message}`);
  }
}

/**
 * Test 5: End-to-End Processing (with timeout)
 */
async function testEndToEndProcessing() {
  console.log('\n📋 Test 5: End-to-End Processing');
  try {
    if (!publishedMessageId) {
      throw new Error('No published message ID available from Test 4');
    }

    const client = new Client(config.postgres);
    await client.connect();

    const initialCount = await client.query('SELECT COUNT(*) FROM messages;');
    const initialTotal = parseInt(initialCount.rows[0].count);

    console.log(`  ℹ️  Initial message count: ${initialTotal}`);

    const processedMessage = await waitForCondition(async () => {
      const result = await client.query(
        "SELECT * FROM messages WHERE content LIKE $1 AND status = 'success' LIMIT 1;",
        [`%${publishedMessageId}%`]
      );

      return result.rows[0] || null;
    }, 10000);

    if (!processedMessage) {
      throw new Error(`Message ${publishedMessageId} was not persisted as successful`);
    }

    const finalCount = await client.query('SELECT COUNT(*) FROM messages;');
    const finalTotal = parseInt(finalCount.rows[0].count);

    console.log(`  ℹ️  Final message count: ${finalTotal}`);
    console.log(`  ℹ️  Messages processed: ${finalTotal - initialTotal}`);
    console.log(`  ✅ Message ${publishedMessageId} was persisted successfully`);
    testsPassed++;
    console.log('  Result: PASSED ✓');

    await client.end();
  } catch (error) {
    testsFailed++;
    console.log(`  ❌ Error: ${error.message}`);
  }
}

/**
 * Test 6: PostgreSQL CRUD Helpers
 */
async function testDatabaseCrudHelpers() {
  console.log('\n📋 Test 6: PostgreSQL CRUD Helpers');
  try {
    const content = `crud-test-${uuidv4()}`;

    const inserted = await insertMessage(content, 'retry', 2);
    if (!inserted || inserted.content !== content || inserted.status !== 'retry') {
      throw new Error('insertMessage did not return the inserted retry message');
    }

    const fetched = await getMessageById(inserted.id);
    if (!fetched || fetched.id !== inserted.id) {
      throw new Error('getMessageById did not return the inserted message');
    }

    const failedMessages = await getFailedMessages(20, 0);
    if (!failedMessages.some((message) => message.id === inserted.id)) {
      throw new Error('getFailedMessages did not include the unprocessed retry message');
    }

    const updated = await updateMessageStatus(inserted.id, 'success', 3);
    if (!updated || updated.status !== 'success' || !updated.processed_at) {
      throw new Error('updateMessageStatus did not mark the message as successful');
    }

    const allMessages = await getAllMessages(20, 0);
    if (!allMessages.some((message) => message.id === inserted.id)) {
      throw new Error('getAllMessages did not include the updated message');
    }

    const deleted = await deleteMessage(inserted.id);
    if (!deleted || deleted.id !== inserted.id) {
      throw new Error('deleteMessage did not return the deleted message');
    }

    const afterDelete = await getMessageById(inserted.id);
    if (afterDelete) {
      throw new Error('deleteMessage did not remove the message');
    }

    console.log('  ✅ insertMessage works');
    console.log('  ✅ getMessageById works');
    console.log('  ✅ getFailedMessages works');
    console.log('  ✅ updateMessageStatus works');
    console.log('  ✅ getAllMessages works');
    console.log('  ✅ deleteMessage works');
    testsPassed++;
    console.log('  Result: PASSED ✓');
  } catch (error) {
    testsFailed++;
    console.log(`  ❌ Error: ${error.message}`);
  }
}

/**
 * Test 7: Queue Statistics
 */
async function testQueueStatistics() {
  console.log('\n📋 Test 7: Queue Statistics');
  try {
    const url = `amqp://${config.rabbitmq.host}:${config.rabbitmq.port}`;
    const connection = await amqp.connect(url);
    const channel = await connection.createChannel();

    const queues = ['main.queue', 'retry.queue', 'deadletter.queue'];
    let allQueuesHealthy = true;

    for (const queueName of queues) {
      const result = await channel.checkQueue(queueName);
      console.log(`  ℹ️  ${queueName}: ${result.messageCount} messages, ${result.consumerCount} consumers`);
    }

    testsPassed++;
    console.log('  Result: PASSED ✓');

    await channel.close();
    await connection.close();
  } catch (error) {
    testsFailed++;
    console.log(`  ❌ Error: ${error.message}`);
  }
}

/**
 * Main Test Runner
 */
async function runTests() {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║   Production-Ready Queue System - Integration Test Suite       ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');

  console.log('\n⏱️  Starting tests...\n');

  await testHealthChecks();
  await testRabbitMQConnection();
  await testPostgresConnection();
  await testProducerMessage();
  await testEndToEndProcessing();
  await testDatabaseCrudHelpers();
  await testQueueStatistics();

  console.log('\n╔════════════════════════════════════════════════════════════════╗');
  console.log(`║  Tests Passed: ${testsPassed}                                                  ║`);
  console.log(`║  Tests Failed: ${testsFailed}                                                  ║`);
  console.log('╚════════════════════════════════════════════════════════════════╝\n');

  await closePool();
  process.exit(testsFailed > 0 ? 1 : 0);
}

runTests().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
