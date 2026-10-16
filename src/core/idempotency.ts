import crypto from 'crypto';
import { redis } from '../config/redis.js';

export type IdempotencyStatus = 'STARTED' | 'QUEUED' | 'DISPATCHED' | 'FAILED';

export interface IdempotencyRecord {
  status: IdempotencyStatus;
  payloadHash: string;
  statusCode?: number;
  responseBody?: Record<string, unknown>;
  createdAt: number;
  error?: string;
}

const IDEMPOTENCY_TTL_SECONDS = 60 * 60 * 24;

export class IdempotencyService {
  private getRedisKey(idempotencyKey: string): string {
    return `idempotency:${idempotencyKey}`;
  }

  public hashPayload(payload: unknown): string {
    const serialized = JSON.stringify(payload ?? {});
    return crypto.createHash('sha256').update(serialized).digest('hex');
  }

  public async acquireLock(
    idempotencyKey: string,
    payload: unknown
  ): Promise<{ acquired: boolean; record: IdempotencyRecord }> {
    const key = this.getRedisKey(idempotencyKey);
    const payloadHash = this.hashPayload(payload);

    const initialRecord: IdempotencyRecord = {
      status: 'STARTED',
      payloadHash,
      createdAt: Date.now(),
    };

    const result = await redis.set(
      key,
      JSON.stringify(initialRecord),
      'EX',
      IDEMPOTENCY_TTL_SECONDS,
      'NX'
    );

    if (result === 'OK') {
      return { acquired: true, record: initialRecord };
    }

    const existingRaw = await redis.get(key);
    if (!existingRaw) {
      // Edge case: key expired in the microsecond between SET NX and GET
      return this.acquireLock(idempotencyKey, payload);
    }

    return {
      acquired: false,
      record: JSON.parse(existingRaw) as IdempotencyRecord,
    };
  }

  /**
   * Called after the API successfully publishes the job to RabbitMQ.
   * Saves the HTTP response so future duplicate retries get the exact same response.
   */
  public async markQueued(
    idempotencyKey: string,
    payloadHash: string,
    statusCode: number,
    responseBody: Record<string, unknown>
  ): Promise<void> {
    const key = this.getRedisKey(idempotencyKey);
    const existingRaw = await redis.get(key);

    let status: IdempotencyStatus = 'QUEUED';
    let error: string | undefined;

    if (existingRaw) {
      const existing = JSON.parse(existingRaw) as IdempotencyRecord;
      if (existing.status === 'DISPATCHED' || existing.status === 'FAILED') {
        status = existing.status;
        error = existing.error;
      }
    }

    const updatedRecord: IdempotencyRecord = {
      status,
      payloadHash,
      statusCode,
      responseBody,
      createdAt: Date.now(),
      ...(error ? { error } : {}),
    };

    await redis.set(key, JSON.stringify(updatedRecord), 'KEEPTTL');
  }

  // Called by the RabbitMQ Worker right after the mock provider succeeds.
  public async markDispatched(idempotencyKey: string): Promise<void> {
    const key = this.getRedisKey(idempotencyKey);
    const existingRaw = await redis.get(key);

    if (existingRaw) {
      const record = JSON.parse(existingRaw) as IdempotencyRecord;
      record.status = 'DISPATCHED';
      await redis.set(key, JSON.stringify(record), 'KEEPTTL');
    }
  }

  //  Used by the RabbitMQ Worker to check if a redelivered message was already sent.
  public async isAlreadyDispatched(idempotencyKey: string): Promise<boolean> {
    const key = this.getRedisKey(idempotencyKey);
    const existingRaw = await redis.get(key);
    if (!existingRaw) return false;

    const record = JSON.parse(existingRaw) as IdempotencyRecord;
    return record.status === 'DISPATCHED';
  }

  // Called by the RabbitMQ Worker when all retries are exhausted before routing to DLQ.
  public async markFailed(idempotencyKey: string, reason: string): Promise<void> {
    const key = this.getRedisKey(idempotencyKey);
    const existingRaw = await redis.get(key);

    if (existingRaw) {
      const record = JSON.parse(existingRaw) as IdempotencyRecord;
      record.status = 'FAILED';
      record.error = reason;
      await redis.set(key, JSON.stringify(record), 'KEEPTTL');
    }
  }

  // Queries the current idempotency record for status polling.
  public async getRecord(idempotencyKey: string): Promise<IdempotencyRecord | null> {
    const key = this.getRedisKey(idempotencyKey);
    const existingRaw = await redis.get(key);
    if (!existingRaw) return null;
    return JSON.parse(existingRaw) as IdempotencyRecord;
  }

  public async releaseLock(idempotencyKey: string): Promise<void> {
    const key = this.getRedisKey(idempotencyKey);
    await redis.del(key);
  }
}

export const idempotencyService = new IdempotencyService();