using MessageForge.Persistence.Outbox;
using Microsoft.EntityFrameworkCore.Storage;

namespace MessageForge.Persistence.UnitOfWork;

internal sealed class EfUnitOfWork<TDbContext> : IUnitOfWork
    where TDbContext : MessageForgeOutboxDbContext
{
    private readonly TDbContext _dbContext;
    private IDbContextTransaction? _transaction;

    public EfUnitOfWork(TDbContext dbContext)
    {
        _dbContext = dbContext;
    }

    /// <inheritdoc />
    public async Task BeginAsync(CancellationToken cancellationToken = default)
    {
        if (_transaction != null)
        {
            throw new InvalidOperationException("A unit of work has already been begun.");
        }

        _transaction = await _dbContext.Database.BeginTransactionAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (_transaction == null)
        {
            throw new InvalidOperationException("BeginAsync must be called before CommitAsync.");
        }

        try
        {
            await _dbContext.SaveChangesAsync(cancellationToken);
            await _transaction.CommitAsync(cancellationToken);
        }
        finally
        {
            await DisposeTransactionAsync();
        }
    }

    /// <inheritdoc />
    public async Task RollbackAsync(CancellationToken cancellationToken = default)
    {
        if (_transaction == null)
        {
            throw new InvalidOperationException("BeginAsync must be called before RollbackAsync.");
        }

        try
        {
            await _transaction.RollbackAsync(cancellationToken);
        }
        finally
        {
            await DisposeTransactionAsync();
        }
    }

    private async Task DisposeTransactionAsync()
    {
        if (_transaction != null)
        {
            await _transaction.DisposeAsync();
            _transaction = null;
        }
    }
}
