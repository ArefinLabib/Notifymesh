import { NotificationJobPayload } from '../core/publisher.js';

export class MockNotificationProvider {
  /**
   * Simulates dispatching to external gateways (SendGrid, Twilio, FCM).
   * - Layer 3 Idempotency: Deduplicates if worker crashes after send but before ack.
   * - Deterministic test hook: Content containing "[FAIL_ALWAYS]" triggers 100% failure for DLQ testing.
   * - Configurable transient failure: Simulates network/gateway outages via MOCK_FAILURE_RATE (default 30%).
   */
  private dispatchedKeys = new Set<string>();

  public async send(job: NotificationJobPayload): Promise<void> {
    // Layer 3 Idempotency Check
    if (this.dispatchedKeys.has(job.idempotencyKey)) {
      console.log(
        `[Provider Deduplication] Gateway already sent ${job.idempotencyKey}. Suppressing duplicate send to ${job.recipient}.`
      );
      return;
    }

    // Simulate 150ms network latency
    await new Promise((res) => setTimeout(res, 150));

    // Deterministic failure hook for testing Dead-Letter Queue (DLQ)
    if (job.content.includes('[FAIL_ALWAYS]')) {
      throw new Error(`Provider outage: Forced permanent failure for ${job.recipient}`);
    }

    // Configurable random transient failure
    const failureRate = Number(process.env.MOCK_FAILURE_RATE ?? 0.3);
    if (Math.random() < failureRate) {
      throw new Error(`Transient 503 error from ${job.channel.toUpperCase()} provider gateway`);
    }

    // Mark as dispatched on external gateway
    this.dispatchedKeys.add(job.idempotencyKey);

    console.log(
      `[Provider] Sent ${job.channel.toUpperCase()} to ${job.recipient} | Key: ${job.idempotencyKey}`
    );
  }
}

export const mockProvider = new MockNotificationProvider();
