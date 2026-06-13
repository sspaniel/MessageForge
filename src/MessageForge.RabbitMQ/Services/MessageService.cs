using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Helpers;
using MessageForge.RabbitMQ.Lifecycle;
using MessageForge.RabbitMQ.Subscribers;
using Microsoft.Extensions.Hosting;

namespace MessageForge.RabbitMQ.Services;

internal sealed class MessageService : IHostedService
{
    internal const string DeadLetterQueueName = "MessageForge.DeadLetter";

    internal const string DeadLetterExchangeName = "MessageForge.DeadLetterExchange";

    internal const string ErrorQueueName = "MessageForge.Errors";

    private readonly IServiceProvider _serviceProvider;

    private readonly MessageServiceOptions _options;

    private readonly IConnectionPool _connectionPool;

    public MessageService(
        IServiceProvider serviceProvider,
        MessageServiceOptions options,
        IConnectionPool connectionPool)
    {
        _serviceProvider = serviceProvider;
        _options = options;
        _connectionPool = connectionPool;
    }

    internal ICollection<IRabbitMQSubscriber> Subscribers { get; } = new LinkedList<IRabbitMQSubscriber>();

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var context = new MessageServiceContext
        {
            ServiceProvider = _serviceProvider,
            CancellationToken = cancellationToken,
        };

        await MessageServiceOptions.InvokeHooksAsync(_options.BeforeMessageServiceStartHooks, context);

        var statupConnection = _connectionPool.GetConnection();
        using var startupChannel = await statupConnection.CreateChannelAsync(cancellationToken: cancellationToken);
        await RabbitMQHelper.CreateDefaultExchangesAsync(startupChannel, cancellationToken);
        await RabbitMQHelper.CreateDefaultQueuesAsync(startupChannel, cancellationToken);

        foreach (var subscriptionOptions in _options.SubscriberOptions)
        {
            var subscriber = new RabbitMQSubscriber(subscriptionOptions, _serviceProvider);
            Subscribers.Add(subscriber);
        }

        foreach (var subscriber in Subscribers)
        {
            await subscriber.InitializeAsync(cancellationToken);
            await subscriber.StartAsync(cancellationToken);
        }

        await MessageServiceOptions.InvokeHooksAsync(_options.AfterMessageServiceStartedHooks, context);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        var context = new MessageServiceContext
        {
            ServiceProvider = _serviceProvider,
            CancellationToken = cancellationToken,
        };

        await MessageServiceOptions.InvokeHooksAsync(_options.BeforeMessageServiceStopHooks, context);

        foreach (var subscriber in Subscribers)
        {
            await subscriber.StopAsync(cancellationToken);
        }

        await MessageServiceOptions.InvokeHooksAsync(_options.AfterMessageServiceStoppedHooks, context);
    }
}
