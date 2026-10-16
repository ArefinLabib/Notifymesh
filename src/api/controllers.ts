import { Request, Response } from 'express';
import { idempotencyService } from '../core/idempotency.js';
import { publishNotification } from '../core/publisher.js';

export async function dispatchNotificationController(req: Request, res: Response) {
  const { idempotencyKey, payloadHash } = req;
  const { channel, recipient, content } = req.body;

  // Basic payload validation (if validation fails, release the lock so client can fix it)
  if (!channel || !recipient || !content) {
    await idempotencyService.releaseLock(idempotencyKey);
    return res.status(400).json({
      error: 'Bad Request',
      message: 'Fields "channel", "recipient", and "content" are required.',
    });
  }

  try {
    const jobPayload = {
      idempotencyKey,
      channel, // 'email' | 'sms' | 'push'
      recipient,
      content,
      enqueuedAt: Date.now(),
    };

    await publishNotification(jobPayload);

    const responseBody = {
      status: 'accepted',
      message: 'Notification queued for dispatch.',
      idempotencyKey,
    };

    await idempotencyService.markQueued(
      idempotencyKey,
      payloadHash,
      202,
      responseBody
    );

    return res.status(202).json(responseBody);
  } catch (err) {
    console.error('[Controller Dispatch Error]:', err);

    await idempotencyService.releaseLock(idempotencyKey).catch((unlockErr) => {
      console.error('[Idempotency Unlock Error]:', unlockErr);
    });

    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to enqueue notification. Please retry with the same Idempotency-Key.',
    });
  }
}
