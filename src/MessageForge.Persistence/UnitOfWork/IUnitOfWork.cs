namespace MessageForge.Persistence.UnitOfWork;

/// <summary>
/// Defines a unit of work for managing transactions across multiple operations to ensure atomicity and consistency.
/// </summary>
public interface IUnitOfWork
{
    /// <summary>
    /// Begins a new unit of work, starting a database transaction.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task BeginAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current unit of work, persisting all changes and committing the transaction.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Rolls back the current unit of work, discarding uncommitted changes.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task RollbackAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes an operation within a database transaction using the configured EF Core execution strategy.
    /// Use this when the <c>DbContext</c> is configured with connection resiliency (for example, <c>EnableRetryOnFailure</c>).
    /// </summary>
    /// <param name="operation">The operation to execute before the transaction is committed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes an operation within a database transaction using the configured EF Core execution strategy and returns its result.
    /// Use this when the <c>DbContext</c> is configured with connection resiliency (for example, <c>EnableRetryOnFailure</c>).
    /// </summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="operation">The operation to execute before the transaction is committed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The value returned by <paramref name="operation"/>.</returns>
    Task<TResult> ExecuteAsync<TResult>(Func<CancellationToken, Task<TResult>> operation, CancellationToken cancellationToken = default);
}
