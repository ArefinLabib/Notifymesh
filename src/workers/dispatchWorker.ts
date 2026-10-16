import { ConsumeMessage } from 'amqplib';
import { getChannel, MQ_TOPOLOGY } from '../config/rabbitmq.js';
import { NotificationJobPayload } from '../core/publisher.js';
import { idempotencyService } from '../core/idempotency.js';
import { mockProvider } from '../providers/mockProvider.js';

const getMaxRetries = () => Number(process.env.MAX_RETRIES ?? 3);
const getBaseDelayMs = () => Number(process.env.BASE_DELAY_MS ?? 1000);

export async function startDispatchWorker(): Promise<void> {
  const channel = getChannel();

  await channel.prefetch(5);

  console.log(`[Worker] Listening on ${MQ_TOPOLOGY.MAIN_QUEUE} (Max Retries: ${getMaxRetries()})`);

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
        const maxRetries = getMaxRetries();
        const baseDelayMs = getBaseDelayMs();

        if (currentRetryCount < maxRetries) {
          const nextRetryCount = currentRetryCount + 1;
          const backoffDelayMs = baseDelayMs * Math.pow(2, nextRetryCount);

          console.warn(
            `[Worker] Dispatch failed for ${payload.idempotencyKey} (Attempt ${nextRetryCount}/${maxRetries}). Retrying in ${backoffDelayMs}ms... Reason: ${errorMessage}`
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
            `[Worker] Retries exhausted (${maxRetries}/${maxRetries}) for ${payload.idempotencyKey}. Routing to DLQ! Last error: ${errorMessage}`
          );
          await idempotencyService.markFailed(payload.idempotencyKey, errorMessage);
          channel.nack(msg, false, false);
        }
      }
    },
    { noAck: false }
  );
}
