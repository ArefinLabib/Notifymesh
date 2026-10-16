import { Router } from 'express';
import { notifyMeshRateLimiter } from './middleware/rateLimiter.js';
import { idempotencyGatekeeper } from './middleware/idempotency.js';
import {
  dispatchNotificationController,
  getNotificationStatusController,
} from './controllers.js';

const router = Router();

router.post(
  '/notifications',
  notifyMeshRateLimiter(),
  idempotencyGatekeeper(),
  dispatchNotificationController
);

router.get(
  '/notifications/:idempotencyKey',
  notifyMeshRateLimiter(),
  getNotificationStatusController
);

export default router;
