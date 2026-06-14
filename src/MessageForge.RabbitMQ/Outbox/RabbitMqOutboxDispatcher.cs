using System.Collections.Concurrent;
using MessageForge.Persistence.Outbox;
using MessageForge.Persistence.Services;
using MessageForge.RabbitMQ.ConnectionPools;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace MessageForge.RabbitMQ.Outbox;

internal sealed class RabbitMqOutboxDispatcher(
    IConnectionPool connectionPool,
    OutboxOptions outboxOptions) : IOutboxDispatcher, IAsyncDisposable
{
    private static readonly CreateChannelOptions PublishChannelOptions = new(
        publisherConfirmationsEnabled: true,
        publisherConfirmationTrackingEnabled: true);

    private readonly IConnectionPool _connectionPool = connectionPool;
    private readonly SemaphoreSlim _dispatchLimiter = new(outboxOptions.DispatchConcurrency, outboxOptions.DispatchConcurrency);
    private readonly ConcurrentStack<PublishChannelLease> _channelPool = new();

    /// <inheritdoc />
    public async Task DispatchAsync(string messageType, byte[] payload, CancellationToken cancellationToken = default)
    {
        await _dispatchLimiter.WaitAsync(cancellationToken);

        PublishChannelLease? lease = null;
        var returnToPool = false;

        try
        {
            try
            {
                lease = await RentChannelAsync(cancellationToken);
                await PublishAsync(lease, messageType, payload, cancellationToken);
                returnToPool = true;
            }
            catch (Exception exception) when (exception is not PublishException)
            {
                if (lease is not null)
                {
                    await lease.DisposeAsync();
                    lease = null;
                }

                lease = await CreateChannelAsync(cancellationToken);
                await PublishAsync(lease, messageType, payload, cancellationToken);
                returnToPool = true;
            }
        }
        finally
        {
            if (returnToPool && lease is { Channel.IsOpen: true })
            {
                _channelPool.Push(lease);
            }
            else if (lease is not null)
            {
                await lease.DisposeAsync();
            }

            _dispatchLimiter.Release();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        while (_channelPool.TryPop(out var lease))
        {
            await lease.DisposeAsync();
        }

        _dispatchLimiter.Dispose();
    }

    private static async Task PublishAsync(
        PublishChannelLease lease,
        string messageType,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        lease.Properties.Type = messageType;

        await lease.Channel.BasicPublishAsync(
            exchange: messageType,
            routingKey: string.Empty,
            mandatory: false,
            body: payload,
            basicProperties: lease.Properties,
            cancellationToken: cancellationToken);
    }

    private async Task<PublishChannelLease> RentChannelAsync(CancellationToken cancellationToken)
    {
        while (_channelPool.TryPop(out var lease))
        {
            if (lease.Channel.IsOpen)
            {
                return lease;
            }

            await lease.DisposeAsync();
        }

        return await CreateChannelAsync(cancellationToken);
    }

    private async Task<PublishChannelLease> CreateChannelAsync(CancellationToken cancellationToken)
    {
        var connection = await _connectionPool.GetConnectionAsync(cancellationToken);
        var channel = await connection.CreateChannelAsync(PublishChannelOptions, cancellationToken: cancellationToken);
        return new PublishChannelLease(channel);
    }

    private sealed class PublishChannelLease(IChannel channel) : IAsyncDisposable
    {
        public IChannel Channel { get; } = channel;

        public BasicProperties Properties { get; } = new() { Persistent = true };

        public ValueTask DisposeAsync() => Channel.DisposeAsync();
    }
}
