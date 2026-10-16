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

    await publishNotification(jobPayload);

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

export async function getNotificationStatusController(req: Request, res: Response) {
  const idempotencyKey = req.params.idempotencyKey as string;

  if (!idempotencyKey) {
    return res.status(400).json({
      error: 'Bad Request',
      message: 'Missing required idempotencyKey parameter.',
    });
  }

  try {
    const record = await idempotencyService.getRecord(idempotencyKey);

    if (!record) {
      return res.status(404).json({
        error: 'Not Found',
        message: `No notification found for Idempotency-Key: ${idempotencyKey}`,
      });
    }

    return res.status(200).json({
      idempotencyKey,
      status: record.status,
      createdAt: record.createdAt,
      ...(record.error ? { error: record.error } : {}),
    });
  } catch (err) {
    console.error('[Controller Status Query Error]:', err);
    return res.status(500).json({
      error: 'Internal Server Error',
      message: 'Failed to retrieve notification status.',
    });
  }
}
