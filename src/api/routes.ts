import { Router } from 'express';
import { notifyMeshRateLimiter } from './middleware/rateLimiter.js';
import { idempotencyGatekeeper } from './middleware/idempotency.js';
import { dispatchNotificationController } from './controllers.js';

const router = Router();

router.post(
  '/notifications',
  notifyMeshRateLimiter(),
  idempotencyGatekeeper(),
  dispatchNotificationController
);

export default router;
