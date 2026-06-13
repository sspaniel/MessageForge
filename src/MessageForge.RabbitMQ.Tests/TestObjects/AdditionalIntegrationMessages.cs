namespace MessageForge.RabbitMQ.Tests.TestObjects;

public sealed class HostedMessage
{
    public Guid Guid { get; set; }
}

public sealed class ErrorQueueMessage
{
    public Guid Guid { get; set; }
}

public sealed class NestedErrorMessage
{
    public Guid Guid { get; set; }
}

public sealed class RecoveryMessage
{
    public Guid Guid { get; set; }
}

public enum SampleStatus
{
    Unknown,
    Active,
    Archived,
}

public sealed class OptionalFieldsMessage
{
    public Guid Guid { get; set; }

    public string? OptionalString { get; set; }

    public int? OptionalNumber { get; set; }

    public SampleStatus Status { get; set; }

    public Dictionary<string, int> Counts { get; set; } = new();

    public List<string?> Items { get; set; } = new();
}
