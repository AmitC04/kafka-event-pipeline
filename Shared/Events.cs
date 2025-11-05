namespace EventPipeline.Shared;

public abstract class BaseEvent
{
    public string EventId { get; set; } = Guid.NewGuid().ToString();
    public string EventType { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
}

public class UserCreatedEvent : BaseEvent
{
    public string UserId { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;

    public UserCreatedEvent() => EventType = nameof(UserCreatedEvent);
}

public class OrderPlacedEvent : BaseEvent
{
    public string OrderId { get; set; } = string.Empty;
    public string UserId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Status { get; set; } = "Placed";

    public OrderPlacedEvent() => EventType = nameof(OrderPlacedEvent);
}

public class PaymentSettledEvent : BaseEvent
{
    public string PaymentId { get; set; } = string.Empty;
    public string OrderId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string Status { get; set; } = "Settled";

    public PaymentSettledEvent() => EventType = nameof(PaymentSettledEvent);
}

public class InventoryAdjustedEvent : BaseEvent
{
    public string Sku { get; set; } = string.Empty;
    public int QuantityChange { get; set; }
    public int NewQuantity { get; set; }

    public InventoryAdjustedEvent() => EventType = nameof(InventoryAdjustedEvent);
}

public class DlqEntry
{
    public string EventId { get; set; } = string.Empty;
    public string OriginalPayload { get; set; } = string.Empty;
    public string ErrorMessage { get; set; } = string.Empty;
    public string StackTrace { get; set; } = string.Empty;
    public DateTime FailedAt { get; set; } = DateTime.UtcNow;
}