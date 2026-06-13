using MessageForge.Persistence.Services;
using MessageForge.Publishers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace MessageForge.Persistence.Outbox;

internal sealed class OutboxPublisher : IPublisher
{
    private readonly IServiceProvider _serviceProvider;
    private readonly OutboxOptions _outboxOptions;
    private readonly IOutboxMessageSerializer _serializer;

    public OutboxPublisher(
        IServiceProvider serviceProvider,
        OutboxOptions outboxOptions,
        IOutboxMessageSerializer serializer)
    {
        _serviceProvider = serviceProvider;
        _outboxOptions = outboxOptions;
        _serializer = serializer;
    }

    /// <inheritdoc />
    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : new()
    {
        var dbContext = (MessageForgeOutboxDbContext)_serviceProvider.GetRequiredService(_outboxOptions.DbContextType);
        var messageType = typeof(TMessage).FullName ?? throw new InvalidOperationException($"Message type {typeof(TMessage).Name} has no full name.");
        var outboxMessageId = ResolveOutboxMessageId(message);

        var skipDuplicate = _outboxOptions.EnableDeduplication
                && (await dbContext.OutboxMessages.AnyAsync(message => message.Id == outboxMessageId, cancellationToken));

        if (skipDuplicate)
        {
            return;
        }

        dbContext.OutboxMessages.Add(new OutboxMessage
        {
            Id = outboxMessageId,
            MessageType = messageType,
            Payload = _serializer.Serialize(message),
            CreatedAt = DateTimeOffset.UtcNow,
        });
    }

    private static Guid ResolveOutboxMessageId<TMessage>(TMessage message)
    {
        var idProperty = typeof(TMessage).GetProperty(nameof(OutboxMessage.Id));

        if (idProperty?.PropertyType == typeof(Guid) && idProperty.GetValue(message) is Guid id && id != Guid.Empty)
        {
            return id;
        }

        return Guid.NewGuid();
    }
}
