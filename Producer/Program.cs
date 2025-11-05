using Confluent.Kafka;
using System.Text.Json;
using EventPipeline.Shared;

var config = new ProducerConfig
{
    BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BROKER") ?? "localhost:9092"
};

using var producer = new ProducerBuilder<string, string>(config).Build();
const string topic = "events";

Console.WriteLine($"Producer starting, broker: {config.BootstrapServers}");

// Wait for Kafka to be ready
await Task.Delay(10000);

var random = new Random();
var userIds = new List<string>();

// Produce events continuously
while (true)
{
    try
    {
        var eventType = random.Next(4);
        
        switch (eventType)
        {
            case 0: // UserCreated
                var userId = $"user-{Guid.NewGuid()}";
                userIds.Add(userId);
                var userEvent = new UserCreatedEvent
                {
                    UserId = userId,
                    Email = $"{userId}@example.com",
                    Name = $"User {userId.Substring(0, 8)}"
                };
                await ProduceEvent(producer, topic, userId, userEvent);
                break;

            case 1: // OrderPlaced
                if (userIds.Count > 0)
                {
                    var orderId = $"order-{Guid.NewGuid()}";
                    var orderUserId = userIds[random.Next(userIds.Count)];
                    var orderEvent = new OrderPlacedEvent
                    {
                        OrderId = orderId,
                        UserId = orderUserId,
                        Amount = random.Next(10, 1000)
                    };
                    await ProduceEvent(producer, topic, orderId, orderEvent);
                }
                break;

            case 2: // PaymentSettled
                var paymentId = $"payment-{Guid.NewGuid()}";
                var paymentOrderId = $"order-{Guid.NewGuid()}";
                var paymentEvent = new PaymentSettledEvent
                {
                    PaymentId = paymentId,
                    OrderId = paymentOrderId,
                    Amount = random.Next(10, 1000)
                };
                await ProduceEvent(producer, topic, paymentOrderId, paymentEvent);
                break;

            case 3: // InventoryAdjusted
                var sku = $"SKU-{random.Next(1, 100):D3}";
                var qtyChange = random.Next(-10, 50);
                var inventoryEvent = new InventoryAdjustedEvent
                {
                    Sku = sku,
                    QuantityChange = qtyChange,
                    NewQuantity = Math.Max(0, 100 + qtyChange)
                };
                await ProduceEvent(producer, topic, sku, inventoryEvent);
                break;
        }

        await Task.Delay(random.Next(500, 2000)); // Random delay between events
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error producing event: {ex.Message}");
    }
}

static async Task ProduceEvent<T>(IProducer<string, string> producer, string topic, string key, T evt) where T : BaseEvent
{
    var json = JsonSerializer.Serialize(evt);
    var message = new Message<string, string>
    {
        Key = key,
        Value = json,
        Headers = new Headers
        {
            { "eventType", System.Text.Encoding.UTF8.GetBytes(evt.EventType) },
            { "eventId", System.Text.Encoding.UTF8.GetBytes(evt.EventId) }
        }
    };

    var result = await producer.ProduceAsync(topic, message);
    Console.WriteLine($"[{evt.EventId}] Produced {evt.EventType} to partition {result.Partition.Value} at offset {result.Offset.Value}");
}