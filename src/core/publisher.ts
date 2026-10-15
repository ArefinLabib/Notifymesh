import { getChannel, MQ_TOPOLOGY } from '../config/rabbitmq.js';

export interface NotificationJobPayload {
  idempotencyKey: string;
  channel: 'email' | 'sms' | 'push';
  recipient: string;
  content: string;
  enqueuedAt: number;
}

/**
 * Publishes a notification job to RabbitMQ's Main Queue with disk persistence.
 */
export async function publishNotification(payload: NotificationJobPayload): Promise<void> {
  const channel = getChannel();
  const messageBuffer = Buffer.from(JSON.stringify(payload));

  return new Promise((resolve, reject) => {
    channel.publish(
      MQ_TOPOLOGY.MAIN_EXCHANGE,
      MQ_TOPOLOGY.ROUTING_KEYS.DISPATCH,
      messageBuffer,
      {
        persistent: true,
        contentType: 'application/json',
        headers: {
          'x-retry-count': 0,
        },
      },
      (err) => {
        if (err) {
          return reject(err);
        }
        resolve();
      }
    );
  });
}
