using RabbitMQ.Client;

namespace MessageForge.RabbitMQ.ConnectionPools;

/// <summary>
/// Connection pool for RabbitMQ.
/// </summary>
public interface IConnectionPool : IDisposable
{
    /// <summary>
    /// Gets a connection from the pool.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);
}
