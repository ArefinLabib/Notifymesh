import 'dotenv/config';
import express from 'express';
import { redis } from './config/redis.js';
import { initRabbitMQ } from './config/rabbitmq.js';
import { startDispatchWorker } from './workers/dispatchWorker.js';
import notificationRoutes from './api/routes.js';

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use('/api', notificationRoutes);

async function bootstrap() {
  try {
    await redis.ping();
    console.log('[Redis] Connected successfully');

    await initRabbitMQ();

    await startDispatchWorker();

    app.listen(PORT, () => {
      console.log(`[NotifyMesh] API running on http://localhost:${PORT}`);
    });
  } catch (err) {
    console.error('Failed to bootstrap NotifyMesh:', err);
    process.exit(1);
  }
}

bootstrap();
