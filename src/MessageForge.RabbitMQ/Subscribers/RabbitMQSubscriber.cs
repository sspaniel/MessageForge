using System.Reflection;
using System.Text.Json;
using MessageForge.Errors;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Serializers;
using MessageForge.RabbitMQ.Services;
using MessageForge.Subscribers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace MessageForge.RabbitMQ.Subscribers;

internal class RabbitMQSubscriber : IRabbitMQSubscriber
{
    private readonly SubscriberOptions _options;
    private readonly IConnectionPool _connectionPool;
    private readonly IMessageSerializer _messageSerializer;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<RabbitMQSubscriber> _logger;
    private readonly string _queueName;

    private IChannel? _channel;
    private AsyncEventingBasicConsumer? _rabbitMqConsumer;

    public RabbitMQSubscriber(SubscriberOptions options, IServiceProvider serviceProvider)
    {
        _options = options;
        _serviceProvider = serviceProvider;
        _connectionPool = serviceProvider.GetRequiredService<IConnectionPool>();
        _messageSerializer = serviceProvider.GetRequiredService<IMessageSerializer>();
        _logger = serviceProvider.GetRequiredService<ILogger<RabbitMQSubscriber>>();

        var subscriberName = _options.SubscriberType.FullName ?? throw new ArgumentNullException(nameof(_options.SubscriberType));
        var messageTypeName = _options.MessageType.FullName ?? throw new ArgumentNullException(nameof(_options.MessageType));
        _queueName = $"{subscriberName}:{messageTypeName}";
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var connection = _connectionPool.GetConnection();
        using var channel = await connection.CreateChannelAsync(cancellationToken: cancellationToken);

        await channel.ExchangeDeclareAsync(
            exchange: _options.MessageType.FullName ?? throw new ArgumentNullException(nameof(_options.MessageType)),
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        var queueOptions = new Dictionary<string, object?>
        {
            { "x-dead-letter-exchange", MessageService.DeadLetterExchangeName },
            { "x-dead-letter-routing-key", MessageService.DeadLetterQueueName },
            { "x-queue-type", "quorum" },
        };

        if (_options.Ttl > TimeSpan.Zero)
        {
            queueOptions.Add("x-message-ttl", (int)_options.Ttl.TotalMilliseconds);
        }

        if (_options.MaxCount > 0)
        {
            queueOptions.Add("x-max-length", _options.MaxCount);
        }

        await channel.QueueDeclareAsync(
            queue: _queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: queueOptions,
            cancellationToken: cancellationToken);

        await channel.QueueBindAsync(
            queue: _queueName,
            exchange: _options.MessageType.FullName ?? throw new ArgumentNullException(nameof(_options.MessageType)),
            routingKey: string.Empty,
            cancellationToken: cancellationToken);
    }

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        var connection = _connectionPool.GetConnection();

        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true,
            consumerDispatchConcurrency: _options.MaxConcurrency);

        _channel = await connection.CreateChannelAsync(channelOptions, cancellationToken: cancellationToken);
        _rabbitMqConsumer = new AsyncEventingBasicConsumer(_channel);
        _rabbitMqConsumer.ReceivedAsync += async (sender, eventArgs) => await ConsumeMessageAsync(eventArgs, cancellationToken);
        await _channel.BasicQosAsync(prefetchSize: 0, prefetchCount: _options.MaxConcurrency, global: false, cancellationToken: cancellationToken);
        await _channel.BasicConsumeAsync(queue: _queueName, autoAck: false, consumer: _rabbitMqConsumer, cancellationToken: cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_channel == null)
        {
            return;
        }

        var consumerTags = _rabbitMqConsumer?.ConsumerTags ?? Array.Empty<string>();

        foreach (var consumerTag in consumerTags)
        {
            if (string.IsNullOrEmpty(consumerTag))
            {
                continue;
            }

            await _channel.BasicCancelAsync(consumerTag, cancellationToken: cancellationToken);
        }

        await _channel.DisposeAsync();
        _channel = null;
    }

    private async Task ConsumeMessageAsync(BasicDeliverEventArgs eventArgs, CancellationToken cancellationToken)
    {
        if (_channel == null || _channel.IsClosed)
        {
            return;
        }

        var deliveryCount = eventArgs.BasicProperties.Headers?.TryGetValue("x-delivery-count", out var value) ?? false ? int.Parse(value?.ToString() ?? "0") : 0;
        var retryLimitReached = (_options.MaxRetryCount == 0 && deliveryCount == 1) || deliveryCount >= _options.MaxRetryCount;

        if (retryLimitReached)
        {
            await _channel.BasicRejectAsync(eventArgs.DeliveryTag, requeue: false, cancellationToken);
            return;
        }

        try
        {
            var message = _messageSerializer.Deserialize(_options.MessageType, eventArgs);

            if (message is null)
            {
                await _channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, cancellationToken);
                return;
            }

            using var scope = _serviceProvider.CreateScope();
            var subscriber = scope.ServiceProvider.GetRequiredService(_options.SubscriberType);

            var handleMethod = _options.SubscriberType.GetMethod(
                name: "HandleAsync",
                bindingAttr: BindingFlags.Instance | BindingFlags.Public,
                types: [_options.MessageType, typeof(CancellationToken)]);

            if (handleMethod == null)
            {
                throw new MissingMethodException(_options.SubscriberType.FullName, "HandleAsync");
            }

            var result = handleMethod.Invoke(subscriber, new object?[] { message, cancellationToken });

            if (result is Task taskResult)
            {
                await taskResult.ConfigureAwait(false);
            }
            else if (result is ValueTask valueTaskResult)
            {
                await valueTaskResult.ConfigureAwait(false);
            }
            else
            {
                _logger.LogWarning("HandleAsync on {subscriberType} did not return Task or ValueTask.", _options.SubscriberType.Name);
            }

            await _channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, cancellationToken);
        }
        catch (JsonException error)
        {
            _logger.LogError(error, "Failed to deserialize message of type {messageType}.", _options.MessageType.Name);

            if (_options.SerializerExceptionBehavior == SubscriberSerializerExceptionBehavior.Ignore)
            {
                await _channel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false, cancellationToken);
                return;
            }

            if (_options.SerializerExceptionBehavior == SubscriberSerializerExceptionBehavior.DeadLetter)
            {
                await _channel.BasicRejectAsync(eventArgs.DeliveryTag, requeue: false, cancellationToken);
                return;
            }
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "{subscriberType} failed to process message of type {messageType}.", _options.SubscriberType.Name, _options.MessageType.Name);

            var error = new MessageForgeError(_options.SubscriberType.Name, exception);
            var errorBody = _messageSerializer.Serialize(error);

            await _channel.BasicPublishAsync(
                exchange: typeof(MessageForgeError).FullName ?? throw new ArgumentNullException(nameof(MessageForgeError)),
                routingKey: string.Empty,
                mandatory: true,
                body: errorBody,
                basicProperties: new BasicProperties { Type = typeof(MessageForgeError).FullName ?? throw new ArgumentNullException(nameof(MessageForgeError)), Persistent = true },
                cancellationToken: cancellationToken);

            await Task.Delay(_options.RetryDelay, cancellationToken);

            await _channel.BasicRejectAsync(eventArgs.DeliveryTag, requeue: true, cancellationToken);
        }
    }
}
