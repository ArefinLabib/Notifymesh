import express from 'express';
import notificationRoutes from './api/routes.js';

export const app = express();

app.use(express.json());
app.use('/api', notificationRoutes);
