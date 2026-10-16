import amqp, { ChannelModel, ConfirmChannel } from 'amqplib';

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost:5672';

export const MQ_TOPOLOGY = {
  MAIN_EXCHANGE: 'notifications.exchange',
  DLX_EXCHANGE: 'notifications.dlx',

  MAIN_QUEUE: 'notifications.main_queue',
  WAIT_QUEUE: 'notifications.wait_queue',
  DLQ_QUEUE: 'notifications.dlq',

  ROUTING_KEYS: {
    DISPATCH: 'dispatch',
    RETRY: 'retry',
    DEAD: 'dead',
  },
} as const;

let connection: ChannelModel | null = null;
let confirmChannel: ConfirmChannel | null = null;

export async function initRabbitMQ(): Promise<{
  connection: ChannelModel;
  channel: ConfirmChannel;
}> {
  if (connection && confirmChannel) {
    return { connection, channel: confirmChannel };
  }

  connection = await amqp.connect(RABBITMQ_URL);
  confirmChannel = await connection.createConfirmChannel();

  await confirmChannel!.assertExchange(MQ_TOPOLOGY.MAIN_EXCHANGE, 'direct', {
    durable: true,
  });
  await confirmChannel!.assertExchange(MQ_TOPOLOGY.DLX_EXCHANGE, 'direct', {
    durable: true,
  });

  await confirmChannel!.assertQueue(MQ_TOPOLOGY.DLQ_QUEUE, {
    durable: true,
  });
  await confirmChannel!.bindQueue(
    MQ_TOPOLOGY.DLQ_QUEUE,
    MQ_TOPOLOGY.DLX_EXCHANGE,
    MQ_TOPOLOGY.ROUTING_KEYS.DEAD
  );

  await confirmChannel!.assertQueue(MQ_TOPOLOGY.MAIN_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': MQ_TOPOLOGY.DLX_EXCHANGE,
      'x-dead-letter-routing-key': MQ_TOPOLOGY.ROUTING_KEYS.DEAD,
    },
  });
  await confirmChannel!.bindQueue(
    MQ_TOPOLOGY.MAIN_QUEUE,
    MQ_TOPOLOGY.MAIN_EXCHANGE,
    MQ_TOPOLOGY.ROUTING_KEYS.DISPATCH
  );

  await confirmChannel!.assertQueue(MQ_TOPOLOGY.WAIT_QUEUE, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': MQ_TOPOLOGY.MAIN_EXCHANGE,
      'x-dead-letter-routing-key': MQ_TOPOLOGY.ROUTING_KEYS.DISPATCH,
    },
  });
  await confirmChannel!.bindQueue(
    MQ_TOPOLOGY.WAIT_QUEUE,
    MQ_TOPOLOGY.MAIN_EXCHANGE,
    MQ_TOPOLOGY.ROUTING_KEYS.RETRY
  );

  console.log('[RabbitMQ] Topology initialized (2 Exchanges, 3 Queues)');
  return { connection, channel: confirmChannel! };
}

export function getChannel(): ConfirmChannel {
  if (!confirmChannel) {
    throw new Error('RabbitMQ channel not initialized.');
  }
  return confirmChannel;
}

export async function closeRabbitMQ(): Promise<void> {
  if (confirmChannel) {
    try {
      await confirmChannel.close();
    } catch {
      // Channel already closed
    }
    confirmChannel = null;
  }
  if (connection) {
    try {
      await connection.close();
    } catch {
      // Connection already closed
    }
    connection = null;
  }
}