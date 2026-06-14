namespace MessageForge.Persistence.Services;

internal sealed class OutboxWorkerId : IOutboxWorkerId
{
    public string Value { get; } = $"{Environment.MachineName}:{Guid.NewGuid():N}";
}
