using MessageForge.Persistence.Outbox;
using Microsoft.EntityFrameworkCore;

namespace MessageForge.Persistence.UnitOfWork;

internal sealed class EfUnitOfWork<TDbContext> : IUnitOfWork
    where TDbContext : MessageForgeOutboxDbContext
{
    private readonly TDbContext _dbContext;
    private bool _inProgress;

    public EfUnitOfWork(TDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    /// <inheritdoc />
    public Task ExecuteAsync(Func<CancellationToken, Task> action, CancellationToken cancellationToken = default)
    {
        return ExecuteAsync(
            async ct =>
            {
                await action(ct);
                return true;
            },
            cancellationToken);
    }

    /// <inheritdoc />
    public async Task<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, Task<TResult>> action,
        CancellationToken cancellationToken = default)
    {
        if (_inProgress)
        {
            throw new InvalidOperationException("A unit of work is already in progress.");
        }

        _inProgress = true;

        try
        {
            var strategy = _dbContext.Database.CreateExecutionStrategy();

            return await strategy.ExecuteAsync(async () =>
            {
                await using var transaction = await _dbContext.Database.BeginTransactionAsync(cancellationToken);
                var result = await action(cancellationToken);
                await _dbContext.SaveChangesAsync(cancellationToken);
                await transaction.CommitAsync(cancellationToken);
                return result;
            });
        }
        finally
        {
            _inProgress = false;
        }
    }
}
