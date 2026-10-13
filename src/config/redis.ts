import 'dotenv/config';
import fs from 'node:fs';
import { Redis } from 'ioredis';

// Extend ioredis type definitions for custom Lua commands
declare module 'ioredis' {
  interface RedisCommander<Context> {
    consumeToken(
      key: string,
      capacity: number,
      refillRatePerMs: number,
      requested: number,
      now: number
    ): Promise<[number, number]>;
  }
}

const REDIS_HOST = process.env.REDIS_HOST || '127.0.0.1';
const REDIS_PORT = Number(process.env.REDIS_PORT) || 6379;

export const redis = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  maxRetriesPerRequest: null,
  retryStrategy(times) {
    const delay = Math.min(times * 50, 2000);
    return delay;
  },
});

// Load and register Lua scripts (EVALSHA under the hood)
const TOKEN_BUCKET_LUA = fs.readFileSync(
  new URL('../core/lua/tokenBucket.lua', import.meta.url),
  'utf-8'
);

redis.defineCommand('consumeToken', {
  numberOfKeys: 1,
  lua: TOKEN_BUCKET_LUA,
});

redis.on('connect', () => {
  console.log('Connected to Redis successfully');
});

redis.on('error', (err) => {
  console.error('Redis Connection Error:', err);
});
