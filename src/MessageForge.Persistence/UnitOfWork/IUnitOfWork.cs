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
}
