using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json;
using MessageForge.Persistence.Outbox.Lifecycle;
using MessageForge.Persistence.Services;
using MessageForge.Publishers;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace MessageForge.Persistence.Outbox;

internal sealed class OutboxPublisher : IPublisher
{
    private static readonly ConcurrentDictionary<Type, PropertyInfo?> MessageIdProperties = new();
    private static readonly ConcurrentDictionary<Type, string> MessageTypeNames = new();

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
        
        var messageType = MessageTypeNames.GetOrAdd(
            typeof(TMessage),
            static type => type.FullName ?? throw new InvalidOperationException($"Message type {type.Name} has no full name."));
        
        var outboxMessageId = ResolveOutboxMessageId(message);

        var skipDuplicate = false;

        if (_outboxOptions.EnableDeduplication)
        {
            if (dbContext.OutboxMessages.Local.Any(m => m.Id == outboxMessageId))
            {
                skipDuplicate = true;
            }
            else
            {
                skipDuplicate = await dbContext.OutboxMessages
                    .AsNoTracking()
                    .AnyAsync(m => m.Id == outboxMessageId, cancellationToken);
            }
        }

        if (skipDuplicate)
        {
            return;
        }

        OutboxEnqueueContext? enqueueContext = null;

        try
        {
            enqueueContext = new OutboxEnqueueContext
            {
                ServiceProvider = _serviceProvider,
                Message = message!,
                MessageType = typeof(TMessage),
                OutboxMessageId = outboxMessageId,
                CancellationToken = cancellationToken,
            };

            await OutboxOptions.InvokeHooksAsync(_outboxOptions.BeforeOutboxEnqueueHooks, enqueueContext);

            var payload = _serializer.Serialize(message);

            dbContext.OutboxMessages.Add(new OutboxMessage
            {
                Id = outboxMessageId,
                MessageType = messageType,
                Payload = payload,
                CreatedAt = DateTimeOffset.UtcNow,
            });

            await OutboxOptions.InvokeHooksAsync(_outboxOptions.AfterOutboxEnqueuedHooks, enqueueContext);
        }
        catch (JsonException error)
        {
            await OutboxOptions.InvokeHooksAsync(
                _outboxOptions.OnOutboxSerializeErrorHooks,
                new OutboxErrorContext
                {
                    ServiceProvider = _serviceProvider,
                    Message = message,
                    MessageType = typeof(TMessage),
                    OutboxMessageId = outboxMessageId,
                    Exception = error,
                    CancellationToken = cancellationToken,
                    Activity = enqueueContext?.Activity,
                });

            if (_outboxOptions.SerializerExceptionBehavior == PublisherSerializerExceptionBehavior.Throw)
            {
                throw;
            }
        }
    }

    private static Guid ResolveOutboxMessageId<TMessage>(TMessage message)
    {
        var idProperty = MessageIdProperties.GetOrAdd(
            typeof(TMessage),
            static messageType => messageType.GetProperty(
                nameof(OutboxMessage.Id),
                BindingFlags.Public | BindingFlags.Instance));

        if (idProperty?.PropertyType == typeof(Guid) && idProperty.GetValue(message) is Guid id && id != Guid.Empty)
        {
            return id;
        }

        return Guid.NewGuid();
    }
}
