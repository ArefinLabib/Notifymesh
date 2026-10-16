# NotifyMesh

An idempotent, rate-limited notification dispatch pipeline built with Node.js, Express, Redis, and RabbitMQ. Designed to handle unreliable downstream providers (email, SMS, push) with atomic deduplication, token-bucket throttling, and non-blocking exponential backoff retries.

---

## Architecture Overview

```
                      +-----------------------------+
                      |       Client Request        |
                      +-----------------------------+
                                     |
                                     v
                      +-----------------------------+
                      |   POST /api/notifications   |
                      +-----------------------------+
                                     |
                                     v
                      +-----------------------------+
                      |    Redis Token Bucket       | ---> 429 Too Many Requests
                      |     (Atomic Lua Refill)     |      (Burst & Sustained Limits)
                      +-----------------------------+
                                     |
                                     v
                      +-----------------------------+
                      |  Idempotency Gatekeeper     | ---> 202 Cached (X-Idempotency-Replayed)
                      |  (SHA-256 & SET NX EX Lock) | ---> 422 Payload Conflict
                      +-----------------------------+      ---> 409 Concurrent Lock Contention
                                     |
                                     v
                      +-----------------------------+
                      |    RabbitMQ Main Queue      |
                      +-----------------------------+
                                     |
                                     v
                         [ Dispatch Worker ]
                                     |
                 +-------------------+-------------------+
                 |                                       |
           Success (200)                           Failure (503 / Timeout)
                 |                                       |
                 v                                       v
      +---------------------+               +-------------------------+
      |  Mark DISPATCHED    |               |  Under Retry Limit?     |
      |   (Ack Message)     |               +-------------------------+
      +---------------------+                     |             |
                                                Yes             No
                                                  |             |
                                                  v             v
                                     +------------------+ +-----------------+
                                     | Wait Queue (TTL) | | Dead-Letter     |
                                     | Dead-letters to  | | Queue (DLQ)     |
                                     | Main Exchange    | | Mark FAILED     |
                                     +------------------+ +-----------------+
```

---

## Core Engineering Decisions

### 1. Atomic Token-Bucket Rate Limiting (Redis + Lua)
Fixed-window and sliding-window limiters suffer from boundary burst spikes or excessive memory footprint. NotifyMesh implements a continuous token-bucket algorithm directly in Redis via an atomic Lua script (`src/core/lua/tokenBucket.lua`):
- **Lazy Refill**: Refills tokens proportionally to elapsed microseconds since the last request, eliminating background timer jobs.
- **Atomic Evaluation**: Reads state, calculates fractional refill, checks availability, decrements, and updates timestamps within a single Redis execution context to eliminate race conditions under high concurrency.
- **Dynamic TTL**: Redis keys auto-expire once the bucket has rested at capacity, preventing key leaks.
- **Two-Tier Thresholds**: Global traffic bounds (100 burst / 25 req/s) paired with client IP bounds (20 burst / 5 req/s).

### 2. Triple-Tier Idempotency Model
To guarantee exactly-once processing semantics across network boundaries:
1. **API Gateway Layer**: Client supplies an `Idempotency-Key` (UUIDv4). The request body is hashed via SHA-256. Redis acquires a distributed lock (`SET NX EX 86400`). Duplicate incoming calls return the cached `202 Accepted` response with an `X-Idempotency-Replayed: true` header without queuing duplicate messages. Payload mismatches for the same key are rejected with `422 Unprocessable Entity`.
2. **Worker Consumer Layer**: RabbitMQ redeliveries check Redis key status before invoking providers. If marked `DISPATCHED`, the message is immediately acknowledged (`ack`) and skipped.
3. **Provider Layer**: The dispatch provider retains an internal idempotency ledger to guard against edge-case redeliveries during network partitions.

### 3. Native Delayed Retries Without Third-Party Plugins
RabbitMQ lacks a native delayed delivery mechanism without external plugins like `rabbitmq-delayed-message-exchange`. NotifyMesh utilizes the native Dead-Letter Exchange (DLX) loop:
- Transient failures publish the message to `notifications.wait_queue` with a calculated `expiration` (per-message TTL using exponential backoff: $2^n \times \text{BASE\_DELAY\_MS}$).
- The wait queue has no active consumers. Once the message TTL expires, RabbitMQ dead-letters the message back to `notifications.exchange` with routing key `dispatch`, placing it back at the tail of `notifications.main_queue`.
- When retries exceed `MAX_RETRIES`, the worker issues a negative acknowledgement without requeue (`nack(msg, false, false)`), cleanly routing the poison message to `notifications.dlq` and updating the status to `FAILED`.

### 4. Lifecycle Observability
Because the HTTP ingestion endpoint is strictly asynchronous (`202 Accepted`), clients can query the processing lifecycle via:
```http
GET /api/notifications/:idempotencyKey
```
Lifecycle states: `STARTED` $\rightarrow$ `QUEUED` $\rightarrow$ `DISPATCHED` | `FAILED`.

---

## Getting Started

### Prerequisites
- Node.js (v20+)
- Docker & Docker Compose

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/ArefinLabib/Notifymesh.git
cd Notifymesh
npm install
```

### 2. Configure Environment
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

Default configuration:
```env
PORT=3000
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
RABBITMQ_URL=amqp://guest:guest@localhost:5672
MAX_RETRIES=3
BASE_DELAY_MS=1000
MOCK_FAILURE_RATE=0.3
```

### 3. Start Infrastructure
Run Redis and RabbitMQ via Docker Compose:
```bash
docker compose up -d
```
- Redis: `localhost:6379`
- RabbitMQ AMQP: `localhost:5672`
- RabbitMQ Management Dashboard: [http://localhost:15672](http://localhost:15672) (User: `guest` / Pass: `guest`)

### 4. Start Development Server
```bash
npm run dev
```

---

## API Reference

### 1. Dispatch Notification
```http
POST /api/notifications
```

**Headers:**
| Header | Type | Description |
| :--- | :--- | :--- |
| `Content-Type` | `string` | Must be `application/json` |
| `Idempotency-Key` | `string` | Unique UUIDv4 transaction identifier |

**Request Body:**
```json
{
  "channel": "email",
  "recipient": "dev@example.com",
  "content": "Your authentication code is 492011"
}
```

**Responses:**
- `202 Accepted`: Message accepted and queued for worker dispatch.
- `202 Accepted` + `X-Idempotency-Replayed: true`: Cached idempotency replay.
- `400 Bad Request`: Missing fields or invalid UUID format.
- `422 Unprocessable Entity`: Idempotency key reused with mismatched payload.
- `429 Too Many Requests`: Rate limit exceeded (`Retry-After` header included).

---

### 2. Query Notification Status
```http
GET /api/notifications/:idempotencyKey
```

**Response (Success):**
```json
{
  "idempotencyKey": "c0ffee00-1234-4000-8000-000000000001",
  "status": "DISPATCHED",
  "createdAt": 1729094000000
}
```

**Response (Retries Exhausted / DLQ):**
```json
{
  "idempotencyKey": "deadbeef-1234-4000-8000-000000000002",
  "status": "FAILED",
  "createdAt": 1729094000000,
  "error": "Provider outage: Forced permanent failure for +123456789"
}
```

---

## Automated Testing

The integration test suite runs via Vitest and Supertest against live Redis and RabbitMQ test containers. It accelerates exponential backoff to `50ms` dynamically so the full multi-tier retry and DLQ lifecycle executes in $< 3$ seconds.

Run tests:
```bash
npm test
```

Typecheck:
```bash
npm run check
```

---

## Project Structure

```
NotifyMesh/
├── src/
│   ├── api/
│   │   ├── middleware/
│   │   │   ├── idempotency.ts    # Gatekeeper distributed locking & payload hashing
│   │   │   └── rateLimiter.ts    # Token-bucket enforcement middleware
│   │   ├── controllers.ts        # Dispatch & status lookup handlers
│   │   └── routes.ts             # Route definitions
│   ├── config/
│   │   ├── rabbitmq.ts           # Topology setup (exchanges, queues, bindings)
│   │   └── redis.ts              # Redis client & custom Lua command registration
│   ├── core/
│   │   ├── lua/
│   │   │   └── tokenBucket.lua   # Atomic token-bucket algorithm
│   │   ├── idempotency.ts        # Redis idempotency service
│   │   └── publisher.ts          # ConfirmChannel message publisher
│   ├── providers/
│   │   └── mockProvider.ts       # Chaos-tested provider simulator
│   ├── workers/
│   │   └── dispatchWorker.ts     # Consumer, exponential backoff, DLQ router
│   ├── app.ts                    # Express app definition (detached for Supertest)
│   └── index.ts                  # Infrastructure bootstrapper & server listener
├── tests/
│   └── notifications.test.ts     # End-to-end integration tests
├── docker-compose.yml            # Redis & RabbitMQ management containers
└── tsconfig.json
```
