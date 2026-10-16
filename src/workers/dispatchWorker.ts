import { ConsumeMessage } from 'amqplib';
import { getChannel, MQ_TOPOLOGY } from '../config/rabbitmq.js';
import { NotificationJobPayload } from '../core/publisher.js';
import { idempotencyService } from '../core/idempotency.js';
import { mockProvider } from '../providers/mockProvider.js';

const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? 3);
const BASE_DELAY_MS = Number(process.env.BASE_DELAY_MS ?? 1000);

export async function startDispatchWorker(): Promise<void> {
  const channel = getChannel();

  await channel.prefetch(5);

  console.log(`[Worker] Listening on ${MQ_TOPOLOGY.MAIN_QUEUE} (Max Retries: ${MAX_RETRIES})`);

  await channel.consume(
    MQ_TOPOLOGY.MAIN_QUEUE,
    async (msg: ConsumeMessage | null) => {
      if (!msg) return;

      const payload: NotificationJobPayload = JSON.parse(msg.content.toString());
      const headers = msg.properties.headers || {};
      const currentRetryCount = Number(headers['x-retry-count'] ?? 0);

      try {
        const alreadySent = await idempotencyService.isAlreadyDispatched(
          payload.idempotencyKey
        );

        if (alreadySent) {
          console.log(
            `[Worker] Skipping ${payload.idempotencyKey} (Already marked DISPATCHED in Redis)`
          );
          channel.ack(msg);
          return;
        }

        await mockProvider.send(payload);
        await idempotencyService.markDispatched(payload.idempotencyKey);
        channel.ack(msg);

      } catch (err: unknown) {
        const errorMessage = err instanceof Error ? err.message : 'Unknown provider error';

        // Evaluate Retry vs Dead-Letter Queue (DLQ)
        if (currentRetryCount < MAX_RETRIES) {
          const nextRetryCount = currentRetryCount + 1;
          const backoffDelayMs = BASE_DELAY_MS * Math.pow(2, nextRetryCount);

          console.warn(
            `[Worker] Dispatch failed for ${payload.idempotencyKey} (Attempt ${nextRetryCount}/${MAX_RETRIES}). Retrying in ${backoffDelayMs}ms... Reason: ${errorMessage}`
          );

          // Publish to Wait Queue with per-message TTL expiration
          channel.publish(
            MQ_TOPOLOGY.MAIN_EXCHANGE,
            MQ_TOPOLOGY.ROUTING_KEYS.RETRY,
            msg.content,
            {
              persistent: true,
              contentType: 'application/json',
              expiration: backoffDelayMs.toString(),
              headers: {
                ...headers,
                'x-retry-count': nextRetryCount,
                'x-last-error': errorMessage,
              },
            }
          );

          channel.ack(msg);
        } else {
          console.error(
            `[Worker] Retries exhausted (${MAX_RETRIES}/${MAX_RETRIES}) for ${payload.idempotencyKey}. Routing to DLQ! Last error: ${errorMessage}`
          );
          channel.nack(msg, false, false);
        }
      }
    },
    { noAck: false }
  );
}
