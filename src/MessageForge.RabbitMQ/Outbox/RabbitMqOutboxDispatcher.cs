using MessageForge.Persistence.Outbox;
using MessageForge.RabbitMQ.ConnectionPools;

namespace MessageForge.RabbitMQ.Outbox;

internal sealed class RabbitMqOutboxDispatcher : IOutboxDispatcher
{
    private readonly IConnectionPool _connectionPool;

    public RabbitMqOutboxDispatcher(IConnectionPool connectionPool)
    {
        _connectionPool = connectionPool;
    }

    /// <inheritdoc />
    public Task DispatchAsync(string messageType, byte[] payload, CancellationToken cancellationToken = default)
    {
        return Publishers.RabbitMqMessagePublisher.PublishRawAsync(
            _connectionPool,
            messageType,
            payload,
            cancellationToken);
    }
}
