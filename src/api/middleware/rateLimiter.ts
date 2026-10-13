import { Request, Response, NextFunction } from 'express';
import { redis } from '../../config/redis.js';

interface TierConfig {
  capacity: number;
  refillRatePerSec: number;
}

const CLIENT_LIMIT: TierConfig = { capacity: 20, refillRatePerSec: 5 }; 
const GLOBAL_LIMIT: TierConfig = { capacity: 100, refillRatePerSec: 25 };

export function notifyMeshRateLimiter() {
  return async (req: Request, res: Response, next: NextFunction) => {
    const clientId = (req.headers['x-client-id'] as string) || req.ip || 'anonymous';
    const clientKey = `ratelimit:client:${clientId}`;
    const globalKey = 'ratelimit:global';
    const now = Date.now();

    try {
      // 1. Evaluate Global Limit first to protect infrastructure
      const [globalAllowed, globalRemaining] = await redis.consumeToken(
        globalKey,
        GLOBAL_LIMIT.capacity,
        GLOBAL_LIMIT.refillRatePerSec / 1000,
        1,
        now
      );

      if (globalAllowed === 0) {
        res.setHeader('Retry-After', '1');
        return res.status(429).json({
          error: 'System Under Heavy Load',
          message: 'Global notification throughput exceeded. Back off and retry.',
        });
      }

      // 2. Evaluate Client Limit
      const [clientAllowed, clientRemaining] = await redis.consumeToken(
        clientKey,
        CLIENT_LIMIT.capacity,
        CLIENT_LIMIT.refillRatePerSec / 1000,
        1,
        now
      );

      res.setHeader('X-RateLimit-Limit', CLIENT_LIMIT.capacity);
      res.setHeader('X-RateLimit-Remaining', clientRemaining);

      if (clientAllowed === 0) {
        const retryAfterSec = Math.ceil(1 / CLIENT_LIMIT.refillRatePerSec);
        res.setHeader('Retry-After', retryAfterSec.toString());
        return res.status(429).json({
          error: 'Too Many Requests',
          message: 'Client dispatch rate exceeded.',
        });
      }

      next();
    } catch (err) {
      // Fail-open or Fail-closed strategy:
      // In critical dispatch pipelines, logging and failing open prevents Redis blips from dropping valid alerts.
      console.error('Rate limiter evaluation error:', err);
      next();
    }
  };
}