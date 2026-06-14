using MessageForge.Persistence.Outbox;
using MessageForge.RabbitMQ.ConnectionPools;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace MessageForge.RabbitMQ.Outbox;

internal sealed class RabbitMqOutboxDispatcher(IConnectionPool connectionPool) : IOutboxDispatcher, IAsyncDisposable
{
    private static readonly CreateChannelOptions PublishChannelOptions = new(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true);

    private readonly IConnectionPool _connectionPool = connectionPool;
    private readonly SemaphoreSlim _publishLock = new(1, 1);
    private IChannel? _publishChannel;

    /// <inheritdoc />
    public async Task DispatchAsync(string messageType, byte[] payload, CancellationToken cancellationToken = default)
    {
        await _publishLock.WaitAsync(cancellationToken);

        try
        {
            try
            {
                await PublishAsync(messageType, payload, cancellationToken);
            }
            catch (Exception exception) when (exception is not PublishException)
            {
                await DisposePublishChannelAsync();
                await PublishAsync(messageType, payload, cancellationToken);
            }
        }
        finally
        {
            _publishLock.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _publishLock.WaitAsync();

        try
        {
            await DisposePublishChannelAsync();
        }
        finally
        {
            _publishLock.Release();
            _publishLock.Dispose();
        }
    }

    private async Task PublishAsync(string messageType, byte[] payload, CancellationToken cancellationToken)
    {
        var channel = await GetOrCreatePublishChannelAsync(cancellationToken);

        await channel.BasicPublishAsync(
            exchange: messageType,
            routingKey: string.Empty,
            mandatory: false,
            body: payload,
            basicProperties: new BasicProperties { Type = messageType, Persistent = true },
            cancellationToken: cancellationToken);
    }

    private async Task<IChannel> GetOrCreatePublishChannelAsync(CancellationToken cancellationToken)
    {
        if (_publishChannel is { IsOpen: true })
        {
            return _publishChannel;
        }

        await DisposePublishChannelAsync();

        var connection = await _connectionPool.GetConnectionAsync(cancellationToken);
        _publishChannel = await connection.CreateChannelAsync(PublishChannelOptions, cancellationToken: cancellationToken);
        return _publishChannel;
    }

    private async Task DisposePublishChannelAsync()
    {
        if (_publishChannel is null)
        {
            return;
        }

        await _publishChannel.DisposeAsync();
        _publishChannel = null;
    }
}
