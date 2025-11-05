# Kafka Event Pipeline

A complete event-driven pipeline with Kafka, SQL Server, Redis DLQ, and REST API.

## Architecture

- **Producer**: Generates 4 event types (UserCreated, OrderPlaced, PaymentSettled, InventoryAdjusted)
- **Consumer**: Processes events with idempotent upserts to SQL Server, DLQ on failure
- **API**: REST endpoints to query users and orders
- **DLQ**: Redis-based dead letter queue for failed messages

## Quick Start

### Prerequisites
- Docker & Docker Compose
- .NET 8 SDK (for local development)

### Run Everything

```bash
# Start all services
docker compose up --build

# Wait ~20 seconds for all services to initialize
```

### Project Structure

```
.
├── Shared/
│   ├── Events.cs           # Event models
│   └── Shared.csproj
├── Producer/
│   ├── Program.cs          # Kafka producer
│   └── Producer.csproj
├── Consumer/
│   ├── Program.cs          # Kafka consumer with DLQ
│   └── Consumer.csproj
├── Api/
│   ├── Program.cs          # REST API
│   └── Api.csproj
├── Dockerfile.Producer
├── Dockerfile.Consumer
├── Dockerfile.Api
├── docker-compose.yml
├── schema.sql
└── sample-requests.http
```

## Event Types

### 1. UserCreatedEvent
```json
{
  "eventId": "guid",
  "eventType": "UserCreatedEvent",
  "userId": "user-{guid}",
  "email": "user@example.com",
  "name": "User Name",
  "timestamp": "2025-11-05T10:00:00Z"
}
```

### 2. OrderPlacedEvent
```json
{
  "eventId": "guid",
  "eventType": "OrderPlacedEvent",
  "orderId": "order-{guid}",
  "userId": "user-{guid}",
  "amount": 123.45,
  "status": "Placed",
  "timestamp": "2025-11-05T10:00:00Z"
}
```

### 3. PaymentSettledEvent
```json
{
  "eventId": "guid",
  "eventType": "PaymentSettledEvent",
  "paymentId": "payment-{guid}",
  "orderId": "order-{guid}",
  "amount": 123.45,
  "status": "Settled",
  "timestamp": "2025-11-05T10:00:00Z"
}
```

### 4. InventoryAdjustedEvent
```json
{
  "eventId": "guid",
  "eventType": "InventoryAdjustedEvent",
  "sku": "SKU-001",
  "quantityChange": -5,
  "newQuantity": 95,
  "timestamp": "2025-11-05T10:00:00Z"
}
```

## API Endpoints

### GET /users/{id}
Returns user info with last 5 orders.

```bash
curl http://localhost:5000/users/user-12345678-1234-1234-1234-123456789012
```

Response:
```json
{
  "user": {
    "userId": "user-...",
    "email": "user@example.com",
    "name": "User Name",
    "createdAt": "2025-11-05T10:00:00"
  },
  "recentOrders": [
    {
      "orderId": "order-...",
      "userId": "user-...",
      "amount": 123.45,
      "status": "Placed",
      "placedAt": "2025-11-05T10:01:00"
    }
  ]
}
```

### GET /orders/{id}
Returns order with payment status.

```bash
curl http://localhost:5000/orders/order-12345678-1234-1234-1234-123456789012
```

Response:
```json
{
  "order": {
    "orderId": "order-...",
    "userId": "user-...",
    "amount": 123.45,
    "status": "Placed",
    "placedAt": "2025-11-05T10:01:00"
  },
  "payment": {
    "paymentId": "payment-...",
    "amount": 123.45,
    "status": "Settled",
    "settledAt": "2025-11-05T10:02:00"
  }
}
```

## Idempotency

All database operations use `MERGE` statements with unique `EventId` checks to ensure:
- Multiple deliveries of the same event don't create duplicates
- Retries are safe
- Exactly-once semantics at the database level

## Dead Letter Queue (DLQ)

Failed messages are pushed to Redis with:
- Original payload
- Error message
- Stack trace
- Timestamp

View DLQ entries:

```bash
# Connect to Redis container
docker exec -it <redis-container-id> redis-cli

# List all DLQ entries
LRANGE dlq:events 0 -1

# Count DLQ entries
LLEN dlq:events
```

## Metrics & Logging

Consumer logs every 10 seconds:
- **Messages/sec**: Throughput
- **DLQ Count**: Failed messages
- **DB Latency p95**: 95th percentile database operation time

All log lines include `[eventId]` for correlation.

## Testing

### 1. Watch Producer Logs
```bash
docker compose logs -f producer
```

You'll see events being produced with their IDs.

### 2. Watch Consumer Logs
```bash
docker compose logs -f consumer
```

Look for:
- Processing messages
- Metrics output
- Any errors

### 3. Query API
Use the user/order IDs from logs:

```bash
# Get a specific user (copy ID from producer logs)
curl http://localhost:5000/users/user-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX

# Get a specific order
curl http://localhost:5000/orders/order-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
```

### 4. Verify Idempotency

Stop the consumer, let events pile up, then restart:

```bash
docker compose stop consumer
# Wait 30 seconds
docker compose start consumer
```

Query the same user again - you should see the same data (no duplicates).

### 5. Check DLQ

If you see errors in consumer logs, check Redis:

```bash
docker exec -it $(docker compose ps -q redis) redis-cli LRANGE dlq:events 0 -1
```

## Configuration

Environment variables (set in docker-compose.yml):

- `KAFKA_BROKER`: Kafka bootstrap servers
- `SQL_CONNECTION`: SQL Server connection string
- `REDIS_CONNECTION`: Redis connection string

## Database Schema

Tables:
- **Users**: UserId (PK), Email, Name, CreatedAt, EventId (unique)
- **Orders**: OrderId (PK), UserId (FK), Amount, Status, PlacedAt, EventId (unique)
- **Payments**: PaymentId (PK), OrderId (FK), Amount, Status, SettledAt, EventId (unique)
- **Inventory**: Sku (PK), Quantity, LastAdjustedAt, EventId

## Cleanup

```bash
# Stop all services
docker compose down

# Remove volumes (deletes data)
docker compose down -v
```

## Troubleshooting

### Services not starting
Wait 20-30 seconds - Kafka and SQL Server take time to initialize.

### API returns 404
Make sure events have been produced. Check producer logs for user/order IDs.

### No events in database
Check consumer logs for errors. Verify SQL Server is running:
```bash
docker compose ps
```

### Consumer crashes
Check SQL Server is ready:
```bash
docker compose logs sqlserver | grep "SQL Server is now ready"
```

## Performance Notes

- Producer generates 1-3 events/second (configurable)
- Consumer processes ~100-500 msg/sec depending on hardware
- Database operations are indexed for fast lookups
- Consumer auto-commits offsets only after successful processing

## Timebox: 60 Minutes

Implementation breakdown:
- [15 min] Core event models & Kafka producer
- [20 min] Consumer with SQL upserts & DLQ
- [10 min] REST API endpoints
- [10 min] Docker Compose & configuration
- [5 min] Testing & documentation
