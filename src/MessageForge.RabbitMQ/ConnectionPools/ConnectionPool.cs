using MessageForge.RabbitMQ.Services;
using RabbitMQ.Client;

namespace MessageForge.RabbitMQ.ConnectionPools;

/// <summary>
/// Connection pool for RabbitMQ.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="ConnectionPool"/> class.
/// </remarks>
/// <param name="options">The messaging options to configure the connection pool.</param>
public class ConnectionPool(MessageServiceOptions options) : IConnectionPool
{
    private readonly MessageServiceOptions _options = options;
    private readonly Queue<IConnection> _connections = new();
    private readonly SemaphoreSlim _poolLock = new(1, 1);

    /// <summary>
    /// Disposes the connection pool.
    /// </summary>
    public void Dispose()
    {
        _poolLock.Dispose();

        while (_connections.Count > 0)
        {
            var connection = _connections.Dequeue();
            connection.Dispose();
        }

        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        await _poolLock.WaitAsync(cancellationToken);

        try
        {
            if (_connections.Count >= _options.ConnectionPoolSize)
            {
                var poolConnection = _connections.Dequeue();

                if (poolConnection.IsOpen)
                {
                    _connections.Enqueue(poolConnection);
                    return poolConnection;
                }

                poolConnection.Dispose();
            }

            var factory = new ConnectionFactory
            {
                Uri = new Uri(_options.ConnectionString),
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(3),
                TopologyRecoveryEnabled = true,
                RequestedHeartbeat = TimeSpan.FromSeconds(5),
            };

            var newConnection = await factory.CreateConnectionAsync(cancellationToken);
            _connections.Enqueue(newConnection);
            return newConnection;
        }
        finally
        {
            _poolLock.Release();
        }
    }
}
