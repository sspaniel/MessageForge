using System.Text.Json;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Lifecycle;
using MessageForge.RabbitMQ.Serializers;
using MessageForge.RabbitMQ.Services;

namespace MessageForge.RabbitMQ.Publishers;

internal sealed class Publisher : IPublisher
{
    private readonly MessageServiceOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly IConnectionPool _connectionPool;
    private readonly IMessageSerializer _messageSerializer;

    public Publisher(
        MessageServiceOptions options,
        IServiceProvider serviceProvider,
        IConnectionPool connectionPool,
        IMessageSerializer messageSerializer)
    {
        _options = options;
        _serviceProvider = serviceProvider;
        _connectionPool = connectionPool;
        _messageSerializer = messageSerializer;
    }

    /// <inheritdoc />
    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : new()
    {
        MessagePublishContext? publishContext = null;

        try
        {
            publishContext = new MessagePublishContext
            {
                ServiceProvider = _serviceProvider,
                Message = message!,
                MessageType = typeof(TMessage),
                CancellationToken = cancellationToken,
            };

            await MessageServiceOptions.InvokeHooksAsync(_options.BeforeMessagePublishHooks, publishContext);

            var messageType = typeof(TMessage).FullName ?? throw new ArgumentNullException(nameof(TMessage));
            var jsonBytes = _messageSerializer.Serialize(message);

            await RabbitMqMessagePublisher.PublishRawAsync(
                _connectionPool,
                messageType,
                jsonBytes,
                cancellationToken);

            await MessageServiceOptions.InvokeHooksAsync(_options.AfterMessagePublishedHooks, publishContext);
        }
        catch (JsonException error)
        {
            await MessageServiceOptions.InvokeHooksAsync(
                _options.OnMessageSerializeErrorHooks,
                new MessageErrorContext
                {
                    ServiceProvider = _serviceProvider,
                    Message = message,
                    MessageType = typeof(TMessage),
                    Exception = error,
                    DeliveryCount = 0,
                    WillRetry = false,
                    WillDeadLetter = false,
                    CancellationToken = cancellationToken,
                    Activity = publishContext?.Activity,
                });

            if (_options.PublisherOptions.SerializerExceptionBehavior == PublisherSerializerExceptionBehavior.Throw)
            {
                throw;
            }
        }
        catch (Exception error)
        {
            await MessageServiceOptions.InvokeHooksAsync(
                _options.OnMessagePublishErrorHooks,
                new MessageErrorContext
                {
                    ServiceProvider = _serviceProvider,
                    Message = message,
                    MessageType = typeof(TMessage),
                    Exception = error,
                    DeliveryCount = 0,
                    WillRetry = false,
                    WillDeadLetter = false,
                    CancellationToken = cancellationToken,
                    Activity = publishContext?.Activity,
                });

            throw new MessagePublishException($"Error publishing message of type {typeof(TMessage).Name}.", error);
        }
    }
}
