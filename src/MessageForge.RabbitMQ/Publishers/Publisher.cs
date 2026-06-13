using System.Text.Json;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Lifecycle;
using MessageForge.RabbitMQ.Serializers;
using MessageForge.RabbitMQ.Services;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace MessageForge.RabbitMQ.Publishers;

internal sealed class Publisher : IPublisher
{
    private readonly MessageServiceOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly IConnectionPool _connectionPool;
    private readonly IMessageSerializer _messageSerializer;
    private readonly ILogger<Publisher> _logger;

    public Publisher(
        MessageServiceOptions options,
        IServiceProvider serviceProvider,
        IConnectionPool connectionPool,
        IMessageSerializer messageSerializer,
        ILogger<Publisher> logger)
    {
        _options = options;
        _serviceProvider = serviceProvider;
        _connectionPool = connectionPool;
        _messageSerializer = messageSerializer;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : new()
    {
        try
        {
            var publishContext = new MessagePublishContext
            {
                ServiceProvider = _serviceProvider,
                Message = message!,
                MessageType = typeof(TMessage),
                CancellationToken = cancellationToken,
            };

            await MessageServiceOptions.InvokeHooksAsync(_options.BeforeMessagePublishHooks, publishContext);

            var connection = _connectionPool.GetConnection();

            var channelOptions = new CreateChannelOptions(
                publisherConfirmationsEnabled: true,
                publisherConfirmationTrackingEnabled: true);

            using var channel = await connection.CreateChannelAsync(channelOptions, cancellationToken: cancellationToken);
            var messageType = typeof(TMessage).FullName ?? throw new ArgumentNullException(nameof(TMessage));
            var jsonBytes = _messageSerializer.Serialize(message);

            _logger.LogInformation("Publishing message of type {messageType}.", typeof(TMessage).Name);

            await channel.BasicPublishAsync(
                exchange: messageType,
                routingKey: string.Empty,
                mandatory: false,
                body: jsonBytes,
                basicProperties: new BasicProperties { Type = messageType, Persistent = true },
                cancellationToken: cancellationToken);

            await MessageServiceOptions.InvokeHooksAsync(_options.AfterMessagePublishedHooks, publishContext);
        }
        catch (JsonException error)
        {
            _logger.LogError(error, "Error serializing message of type {messageType}.", typeof(TMessage).Name);

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
                });

            if (_options.PublisherOptions.SerializerExceptionBehavior == PublisherSerializerExceptionBehavior.Throw)
            {
                throw;
            }
        }
        catch (Exception error)
        {
            _logger.LogError(error, "Error publishing message of type {messageType}", typeof(TMessage).Name);

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
                });

            throw new MessagePublishException($"Error publishing message of type {typeof(TMessage).Name}.", error);
        }
    }
}
