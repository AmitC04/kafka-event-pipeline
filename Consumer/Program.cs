using Confluent.Kafka;
using System.Text.Json;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using StackExchange.Redis;
using EventPipeline.Shared;

var kafkaBroker = Environment.GetEnvironmentVariable("KAFKA_BROKER") ?? "localhost:9092";
var sqlConnection = Environment.GetEnvironmentVariable("SQL_CONNECTION") ?? "Server=localhost;Database=EventStore;User Id=sa;Password=YourStrong@Passw0rd;TrustServerCertificate=True";
var redisConnection = Environment.GetEnvironmentVariable("REDIS_CONNECTION") ?? "localhost:6379";

Console.WriteLine("Consumer starting...");

// Wait for dependencies
await Task.Delay(15000);

// Initialize DB
await InitializeDatabase(sqlConnection);

// Setup Redis DLQ
var redis = ConnectionMultiplexer.Connect(redisConnection);
var db = redis.GetDatabase();

var config = new ConsumerConfig
{
    BootstrapServers = kafkaBroker,
    GroupId = "event-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("events");

Console.WriteLine("Consumer subscribed to topic 'events'");

// Metrics
var stopwatch = Stopwatch.StartNew();
long messagesProcessed = 0;
long dlqCount = 0;
var latencies = new List<long>();

while (true)
{
    try
    {
        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(1));
        if (consumeResult == null) continue;

        var sw = Stopwatch.StartNew();
        var eventId = GetHeaderValue(consumeResult.Message.Headers, "eventId");
        var eventType = GetHeaderValue(consumeResult.Message.Headers, "eventType");

        Console.WriteLine($"[{eventId}] Processing {eventType}");

        try
        {
            await ProcessEvent(eventType, consumeResult.Message.Value, sqlConnection);
            consumer.Commit(consumeResult);
            messagesProcessed++;
            
            sw.Stop();
            latencies.Add(sw.ElapsedMilliseconds);
            
            // Keep only last 100 latencies for p95 calculation
            if (latencies.Count > 100) latencies.RemoveAt(0);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{eventId}] ERROR: {ex.Message}");
            await PushToDlq(db, eventId, consumeResult.Message.Value, ex);
            dlqCount++;
            consumer.Commit(consumeResult); // Commit to avoid reprocessing
        }

        // Log metrics every 10 seconds
        if (stopwatch.ElapsedMilliseconds > 10000)
        {
            var msgPerSec = messagesProcessed / (stopwatch.ElapsedMilliseconds / 1000.0);
            var p95 = latencies.Count > 0 ? latencies.OrderBy(x => x).ElementAt((int)(latencies.Count * 0.95)) : 0;
            
            Console.WriteLine($"===== METRICS =====");
            Console.WriteLine($"Messages/sec: {msgPerSec:F2}");
            Console.WriteLine($"DLQ Count: {dlqCount}");
            Console.WriteLine($"DB Latency p95: {p95}ms");
            Console.WriteLine($"==================");
            
            stopwatch.Restart();
            messagesProcessed = 0;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Consumer error: {ex.Message}");
    }
}

static string GetHeaderValue(Headers headers, string key)
{
    var header = headers.FirstOrDefault(h => h.Key == key);
    return header != null ? System.Text.Encoding.UTF8.GetString(header.GetValueBytes()) : string.Empty;
}

static async Task ProcessEvent(string eventType, string payload, string connStr)
{
    await using var conn = new SqlConnection(connStr);
    await conn.OpenAsync();

    switch (eventType)
    {
        case nameof(UserCreatedEvent):
            var userEvent = JsonSerializer.Deserialize<UserCreatedEvent>(payload)!;
            await UpsertUser(conn, userEvent);
            break;

        case nameof(OrderPlacedEvent):
            var orderEvent = JsonSerializer.Deserialize<OrderPlacedEvent>(payload)!;
            await UpsertOrder(conn, orderEvent);
            break;

        case nameof(PaymentSettledEvent):
            var paymentEvent = JsonSerializer.Deserialize<PaymentSettledEvent>(payload)!;
            await UpsertPayment(conn, paymentEvent);
            break;

        case nameof(InventoryAdjustedEvent):
            var inventoryEvent = JsonSerializer.Deserialize<InventoryAdjustedEvent>(payload)!;
            await UpsertInventory(conn, inventoryEvent);
            break;

        default:
            throw new InvalidOperationException($"Unknown event type: {eventType}");
    }
}

static async Task UpsertUser(SqlConnection conn, UserCreatedEvent evt)
{
    var cmd = new SqlCommand(@"
        MERGE Users AS target
        USING (SELECT @UserId AS UserId) AS source
        ON target.UserId = source.UserId
        WHEN MATCHED THEN
            UPDATE SET Email = @Email, Name = @Name
        WHEN NOT MATCHED THEN
            INSERT (UserId, Email, Name, CreatedAt, EventId)
            VALUES (@UserId, @Email, @Name, @CreatedAt, @EventId);
    ", conn);

    cmd.Parameters.AddWithValue("@UserId", evt.UserId);
    cmd.Parameters.AddWithValue("@Email", evt.Email);
    cmd.Parameters.AddWithValue("@Name", evt.Name);
    cmd.Parameters.AddWithValue("@CreatedAt", evt.Timestamp);
    cmd.Parameters.AddWithValue("@EventId", evt.EventId);

    await cmd.ExecuteNonQueryAsync();
}

static async Task UpsertOrder(SqlConnection conn, OrderPlacedEvent evt)
{
    var cmd = new SqlCommand(@"
        IF NOT EXISTS (SELECT 1 FROM Orders WHERE EventId = @EventId)
        BEGIN
            MERGE Orders AS target
            USING (SELECT @OrderId AS OrderId) AS source
            ON target.OrderId = source.OrderId
            WHEN MATCHED THEN
                UPDATE SET Amount = @Amount, Status = @Status
            WHEN NOT MATCHED THEN
                INSERT (OrderId, UserId, Amount, Status, PlacedAt, EventId)
                VALUES (@OrderId, @UserId, @Amount, @Status, @PlacedAt, @EventId);
        END
    ", conn);

    cmd.Parameters.AddWithValue("@OrderId", evt.OrderId);
    cmd.Parameters.AddWithValue("@UserId", evt.UserId);
    cmd.Parameters.AddWithValue("@Amount", evt.Amount);
    cmd.Parameters.AddWithValue("@Status", evt.Status);
    cmd.Parameters.AddWithValue("@PlacedAt", evt.Timestamp);
    cmd.Parameters.AddWithValue("@EventId", evt.EventId);

    await cmd.ExecuteNonQueryAsync();
}

static async Task UpsertPayment(SqlConnection conn, PaymentSettledEvent evt)
{
    var cmd = new SqlCommand(@"
        IF NOT EXISTS (SELECT 1 FROM Payments WHERE EventId = @EventId)
        BEGIN
            MERGE Payments AS target
            USING (SELECT @PaymentId AS PaymentId) AS source
            ON target.PaymentId = source.PaymentId
            WHEN MATCHED THEN
                UPDATE SET Amount = @Amount, Status = @Status
            WHEN NOT MATCHED THEN
                INSERT (PaymentId, OrderId, Amount, Status, SettledAt, EventId)
                VALUES (@PaymentId, @OrderId, @Amount, @Status, @SettledAt, @EventId);
        END
    ", conn);

    cmd.Parameters.AddWithValue("@PaymentId", evt.PaymentId);
    cmd.Parameters.AddWithValue("@OrderId", evt.OrderId);
    cmd.Parameters.AddWithValue("@Amount", evt.Amount);
    cmd.Parameters.AddWithValue("@Status", evt.Status);
    cmd.Parameters.AddWithValue("@SettledAt", evt.Timestamp);
    cmd.Parameters.AddWithValue("@EventId", evt.EventId);

    await cmd.ExecuteNonQueryAsync();
}

static async Task UpsertInventory(SqlConnection conn, InventoryAdjustedEvent evt)
{
    var cmd = new SqlCommand(@"
        MERGE Inventory AS target
        USING (SELECT @Sku AS Sku) AS source
        ON target.Sku = source.Sku
        WHEN MATCHED THEN
            UPDATE SET Quantity = @Quantity, LastAdjustedAt = @AdjustedAt, EventId = @EventId
        WHEN NOT MATCHED THEN
            INSERT (Sku, Quantity, LastAdjustedAt, EventId)
            VALUES (@Sku, @Quantity, @AdjustedAt, @EventId);
    ", conn);

    cmd.Parameters.AddWithValue("@Sku", evt.Sku);
    cmd.Parameters.AddWithValue("@Quantity", evt.NewQuantity);
    cmd.Parameters.AddWithValue("@AdjustedAt", evt.Timestamp);
    cmd.Parameters.AddWithValue("@EventId", evt.EventId);

    await cmd.ExecuteNonQueryAsync();
}

static async Task PushToDlq(IDatabase db, string eventId, string payload, Exception ex)
{
    var dlqEntry = new DlqEntry
    {
        EventId = eventId,
        OriginalPayload = payload,
        ErrorMessage = ex.Message,
        StackTrace = ex.StackTrace ?? string.Empty
    };

    var json = JsonSerializer.Serialize(dlqEntry);
    await db.ListRightPushAsync("dlq:events", json);
}

static async Task InitializeDatabase(string connStr)
{
    var retries = 10;
    while (retries > 0)
    {
        try
        {
            await using var conn = new SqlConnection(connStr);
            await conn.OpenAsync();
            Console.WriteLine("Database connection successful");
            
            // Check if tables exist, create if not
            var cmd = new SqlCommand(@"
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Users')
                BEGIN
                    CREATE TABLE Users (
                        UserId NVARCHAR(100) PRIMARY KEY,
                        Email NVARCHAR(255) NOT NULL,
                        Name NVARCHAR(255) NOT NULL,
                        CreatedAt DATETIME2 NOT NULL,
                        EventId NVARCHAR(100) NOT NULL UNIQUE
                    );
                END
                
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Orders')
                BEGIN
                    CREATE TABLE Orders (
                        OrderId NVARCHAR(100) PRIMARY KEY,
                        UserId NVARCHAR(100) NOT NULL,
                        Amount DECIMAL(18,2) NOT NULL,
                        Status NVARCHAR(50) NOT NULL,
                        PlacedAt DATETIME2 NOT NULL,
                        EventId NVARCHAR(100) NOT NULL UNIQUE
                    );
                    CREATE INDEX IX_Orders_UserId ON Orders(UserId);
                    CREATE INDEX IX_Orders_PlacedAt ON Orders(PlacedAt DESC);
                END
                
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Payments')
                BEGIN
                    CREATE TABLE Payments (
                        PaymentId NVARCHAR(100) PRIMARY KEY,
                        OrderId NVARCHAR(100) NOT NULL,
                        Amount DECIMAL(18,2) NOT NULL,
                        Status NVARCHAR(50) NOT NULL,
                        SettledAt DATETIME2 NOT NULL,
                        EventId NVARCHAR(100) NOT NULL UNIQUE
                    );
                    CREATE INDEX IX_Payments_OrderId ON Payments(OrderId);
                END
                
                IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Inventory')
                BEGIN
                    CREATE TABLE Inventory (
                        Sku NVARCHAR(100) PRIMARY KEY,
                        Quantity INT NOT NULL,
                        LastAdjustedAt DATETIME2 NOT NULL,
                        EventId NVARCHAR(100) NOT NULL
                    );
                END
            ", conn);
            
            await cmd.ExecuteNonQueryAsync();
            Console.WriteLine("Database schema initialized");
            return;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Database init failed: {ex.Message}. Retrying... ({retries} left)");
            retries--;
            await Task.Delay(3000);
        }
    }
    
    throw new Exception("Failed to initialize database");
}