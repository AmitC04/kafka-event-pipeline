using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var sqlConnection = Environment.GetEnvironmentVariable("SQL_CONNECTION") 
    ?? "Server=localhost;Database=EventStore;User Id=sa;Password=YourStrong@Passw0rd;TrustServerCertificate=True";

// Wait for database
await Task.Delay(15000);

app.MapGet("/users/{id}", async (string id) =>
{
    await using var conn = new SqlConnection(sqlConnection);
    await conn.OpenAsync();

    // Get user
    var userCmd = new SqlCommand(@"
        SELECT UserId, Email, Name, CreatedAt 
        FROM Users 
        WHERE UserId = @UserId
    ", conn);
    userCmd.Parameters.AddWithValue("@UserId", id);

    object? user = null;
    await using (var reader = await userCmd.ExecuteReaderAsync())
    {
        if (await reader.ReadAsync())
        {
            user = new
            {
                userId = reader.GetString(0),
                email = reader.GetString(1),
                name = reader.GetString(2),
                createdAt = reader.GetDateTime(3)
            };
        }
    }

    if (user == null)
        return Results.NotFound(new { error = "User not found" });

    // Get last 5 orders
    var ordersCmd = new SqlCommand(@"
        SELECT TOP 5 OrderId, UserId, Amount, Status, PlacedAt
        FROM Orders
        WHERE UserId = @UserId
        ORDER BY PlacedAt DESC
    ", conn);
    ordersCmd.Parameters.AddWithValue("@UserId", id);

    var orders = new List<object>();
    await using (var reader = await ordersCmd.ExecuteReaderAsync())
    {
        while (await reader.ReadAsync())
        {
            orders.Add(new
            {
                orderId = reader.GetString(0),
                userId = reader.GetString(1),
                amount = reader.GetDecimal(2),
                status = reader.GetString(3),
                placedAt = reader.GetDateTime(4)
            });
        }
    }

    return Results.Ok(new
    {
        user,
        recentOrders = orders
    });
});

app.MapGet("/orders/{id}", async (string id) =>
{
    await using var conn = new SqlConnection(sqlConnection);
    await conn.OpenAsync();

    // Get order
    var orderCmd = new SqlCommand(@"
        SELECT OrderId, UserId, Amount, Status, PlacedAt
        FROM Orders
        WHERE OrderId = @OrderId
    ", conn);
    orderCmd.Parameters.AddWithValue("@OrderId", id);

    object? order = null;
    await using (var reader = await orderCmd.ExecuteReaderAsync())
    {
        if (await reader.ReadAsync())
        {
            order = new
            {
                orderId = reader.GetString(0),
                userId = reader.GetString(1),
                amount = reader.GetDecimal(2),
                status = reader.GetString(3),
                placedAt = reader.GetDateTime(4)
            };
        }
    }

    if (order == null)
        return Results.NotFound(new { error = "Order not found" });

    // Get payment status
    var paymentCmd = new SqlCommand(@"
        SELECT PaymentId, Amount, Status, SettledAt
        FROM Payments
        WHERE OrderId = @OrderId
    ", conn);
    paymentCmd.Parameters.AddWithValue("@OrderId", id);

    object? payment = null;
    await using (var reader = await paymentCmd.ExecuteReaderAsync())
    {
        if (await reader.ReadAsync())
        {
            payment = new
            {
                paymentId = reader.GetString(0),
                amount = reader.GetDecimal(1),
                status = reader.GetString(2),
                settledAt = reader.GetDateTime(3)
            };
        }
    }

    return Results.Ok(new
    {
        order,
        payment
    });
});

app.MapGet("/health", () => Results.Ok(new { status = "healthy" }));

Console.WriteLine("API listening on port 8080");
app.Run();