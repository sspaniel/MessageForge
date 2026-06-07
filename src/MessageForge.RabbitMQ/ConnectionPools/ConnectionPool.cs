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
public class ConnectionPool(MessagingServiceOptions options) : IConnectionPool
{
    private readonly MessagingServiceOptions _options = options;
    private readonly Queue<IConnection> _connections = new Queue<IConnection>();

    /// <summary>
    /// Disposes the connection pool.
    /// </summary>
    public void Dispose()
    {
        while (_connections.Count > 0)
        {
            var connection = _connections.Dequeue();
            connection.Dispose();
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Gets a connection from the pool.
    /// </summary>
    public IConnection GetConnection()
    {
        lock (_connections)
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

            var newConnection = factory.CreateConnectionAsync().Result;
            _connections.Enqueue(newConnection);
            return newConnection;
        }
    }
}