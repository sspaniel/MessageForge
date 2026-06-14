namespace MessageForge.Persistence.UnitOfWork;

/// <summary>
/// Defines a unit of work for managing transactions across multiple operations to ensure atomicity and consistency.
/// </summary>
public interface IUnitOfWork
{
    /// <summary>
    /// Executes the supplied work inside a database transaction, persisting all tracked changes before commit.
    /// </summary>
    /// <param name="action">The work to perform inside the transaction.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken = default);

    /// <summary>
    /// Executes the supplied work inside a database transaction, persisting all tracked changes before commit.
    /// </summary>
    /// <typeparam name="TResult">The result returned by <paramref name="action"/>.</typeparam>
    /// <param name="action">The work to perform inside the transaction.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The value returned by <paramref name="action"/>.</returns>
    Task<TResult> ExecuteAsync<TResult>(Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken = default);
}
