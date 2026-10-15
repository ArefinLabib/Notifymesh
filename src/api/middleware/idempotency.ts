import { Request, Response, NextFunction } from 'express';
import { idempotencyService } from '../../core/idempotency.js';

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

declare global {
  namespace Express {
    interface Request {
      idempotencyKey: string;
      payloadHash: string;
    }
  }
}

export function idempotencyGatekeeper() {
  return async (req: Request, res: Response, next: NextFunction) => {
    const idempotencyKey = req.headers['idempotency-key'] as string | undefined;

    if (!idempotencyKey) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Missing required Idempotency-Key header.',
      });
    }

    if (!UUID_REGEX.test(idempotencyKey)) {
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Idempotency-Key must be a valid UUID.',
      });
    }

    try {
      const payloadHash = idempotencyService.hashPayload(req.body);
      const { acquired, record } = await idempotencyService.acquireLock(
        idempotencyKey,
        req.body
      );

      if (!acquired) {
        if (record.payloadHash !== payloadHash) {
          return res.status(422).json({
            error: 'Unprocessable Entity',
            message: 'Idempotency-Key already used with a different request payload.',
          });
        }

        if (record.status === 'STARTED') {
          return res.status(409).json({
            error: 'Conflict',
            message: 'A request with this Idempotency-Key is currently being processed.',
          });
        }

        // Send cached response if QUEUED or DISPATCHED
        res.setHeader('X-Idempotency-Replayed', 'true');
        return res.status(record.statusCode || 202).json(record.responseBody);
      }

      // Attach metadata to request and pass to controller if its a new key
      req.idempotencyKey = idempotencyKey;
      req.payloadHash = payloadHash;
      next();
    } catch (err) {
      // Fail closed so Redis outages cannot cause duplicate sends during retry storms
      console.error('[Idempotency Gatekeeper Error]:', err);
      return res.status(503).json({
        error: 'Service Unavailable',
        message: 'Idempotency verification unavailable. Please retry shortly.',
      });
    }
  };
}
