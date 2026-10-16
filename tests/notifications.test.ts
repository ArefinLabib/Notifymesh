import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';
import request from 'supertest';
import crypto from 'crypto';

// 1. Speed up exponential backoff BEFORE importing worker (50ms base -> 100ms, 200ms, 400ms)
process.env.BASE_DELAY_MS = '50';
process.env.MAX_RETRIES = '3';
process.env.MOCK_FAILURE_RATE = '0'; // Disable random failures for deterministic tests

import { app } from '../src/app.js';
import { redis } from '../src/config/redis.js';
import { initRabbitMQ, getChannel, closeRabbitMQ, MQ_TOPOLOGY } from '../src/config/rabbitmq.js';
import { startDispatchWorker } from '../src/workers/dispatchWorker.js';
import { mockProvider } from '../src/providers/mockProvider.js';

// Helper to wait for async RabbitMQ worker processing
const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

describe('NotifyMesh — Integration Tests', () => {
  beforeAll(async () => {
    await initRabbitMQ();
    await startDispatchWorker();
  });

  beforeEach(async () => {
    // Clear Redis keys and purge all 3 RabbitMQ queues before each test
    await redis.flushall();
    const channel = getChannel();
    await channel.purgeQueue(MQ_TOPOLOGY.MAIN_QUEUE);
    await channel.purgeQueue(MQ_TOPOLOGY.WAIT_QUEUE);
    await channel.purgeQueue(MQ_TOPOLOGY.DLQ_QUEUE);
    vi.restoreAllMocks();
  });

  afterAll(async () => {
    await redis.quit();
    await closeRabbitMQ();
  });

  it('submits the same idempotency key twice and asserts only ONE dispatch occurs', async () => {
    const sendSpy = vi.spyOn(mockProvider, 'send').mockResolvedValue();
    const idempotencyKey = crypto.randomUUID();
    const payload = {
      channel: 'email',
      recipient: 'test@example.com',
      content: 'Welcome to NotifyMesh!',
    };

    // 1. First submission -> Enqueues to RabbitMQ
    const res1 = await request(app)
      .post('/api/notifications')
      .set('Idempotency-Key', idempotencyKey)
      .send(payload);

    expect(res1.status).toBe(202);
    expect(res1.headers['x-idempotency-replayed']).toBeUndefined();

    // 2. Second submission with the SAME Idempotency-Key -> Caught by Redis Gatekeeper
    const res2 = await request(app)
      .post('/api/notifications')
      .set('Idempotency-Key', idempotencyKey)
      .send(payload);

    expect(res2.status).toBe(202);
    expect(res2.headers['x-idempotency-replayed']).toBe('true');

    // Wait briefly for the background RabbitMQ consumer to finish processing
    await sleep(200);

    // 3. Assert provider was called strictly ONCE
    expect(sendSpy).toHaveBeenCalledTimes(1);
    expect(sendSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        idempotencyKey,
        recipient: 'test@example.com',
      })
    );

    // 4. Assert status endpoint reports DISPATCHED
    const statusRes = await request(app).get(`/api/notifications/${idempotencyKey}`);
    expect(statusRes.status).toBe(200);
    expect(statusRes.body.status).toBe('DISPATCHED');
  });

  it('forces a provider failure N+1 times and asserts the message lands on the Dead-Letter Queue', async () => {
    // Force provider to fail every single time
    const sendSpy = vi
      .spyOn(mockProvider, 'send')
      .mockRejectedValue(new Error('Simulated downstream SMS gateway outage'));

    const idempotencyKey = crypto.randomUUID();
    const payload = {
      channel: 'sms',
      recipient: '+15550001111',
      content: 'OTP: 994821',
    };

    // 1. Submit notification
    const res = await request(app)
      .post('/api/notifications')
      .set('Idempotency-Key', idempotencyKey)
      .send(payload);

    expect(res.status).toBe(202);

    // 2. Wait for 1 initial attempt + 3 backoff retries (100ms + 200ms + 400ms = 700ms total + buffer)
    await sleep(1500);

    // 3. Assert provider was attempted exactly N + 1 times (1 initial + 3 retries = 4 calls)
    expect(sendSpy).toHaveBeenCalledTimes(4);

    // 4. Assert message is no longer retrying and now sits inside notifications.dlq
    const channel = getChannel();
    const dlqMessage = await channel.get(MQ_TOPOLOGY.DLQ_QUEUE, { noAck: false });

    expect(dlqMessage).not.toBe(false);
    if (dlqMessage) {
      const deadPayload = JSON.parse(dlqMessage.content.toString());
      expect(deadPayload.idempotencyKey).toBe(idempotencyKey);
      expect(dlqMessage.properties.headers?.['x-retry-count']).toBe(3);
      expect(dlqMessage.properties.headers?.['x-last-error']).toContain(
        'Simulated downstream SMS gateway outage'
      );
      channel.ack(dlqMessage);
    }

    // 5. Assert status endpoint reports FAILED with error
    const statusRes = await request(app).get(`/api/notifications/${idempotencyKey}`);
    expect(statusRes.status).toBe(200);
    expect(statusRes.body.status).toBe('FAILED');
    expect(statusRes.body.error).toContain('Simulated downstream SMS gateway outage');
  });

  it('rejects duplicate idempotency key with different payload with 422', async () => {
    const idempotencyKey = crypto.randomUUID();

    const res1 = await request(app)
      .post('/api/notifications')
      .set('Idempotency-Key', idempotencyKey)
      .send({
        channel: 'email',
        recipient: 'alice@example.com',
        content: 'Original Message',
      });

    expect(res1.status).toBe(202);

    const res2 = await request(app)
      .post('/api/notifications')
      .set('Idempotency-Key', idempotencyKey)
      .send({
        channel: 'email',
        recipient: 'bob@example.com',
        content: 'Tampered Message',
      });

    expect(res2.status).toBe(422);
    expect(res2.body.error).toBe('Unprocessable Entity');
  });
});
