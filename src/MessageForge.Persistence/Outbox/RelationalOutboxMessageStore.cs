using Microsoft.EntityFrameworkCore;

namespace MessageForge.Persistence.Outbox;

internal sealed class RelationalOutboxMessageStore : IOutboxMessageStore
{
    private const string PostgreSqlProvider = "Npgsql.EntityFrameworkCore.PostgreSQL";
    private const string SqlServerProvider = "Microsoft.EntityFrameworkCore.SqlServer";

    private readonly MessageForgeOutboxDbContext _dbContext;
    private readonly string _claimSql;

    public RelationalOutboxMessageStore(MessageForgeOutboxDbContext dbContext)
    {
        _dbContext = dbContext;
        _claimSql = ResolveClaimSql(dbContext.Database.ProviderName);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<OutboxClaimedMessage>> ClaimBatchAsync(
        string workerId,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default)
    {
        var now = DateTimeOffset.UtcNow;
        var lockedUntil = now.Add(leaseDuration);

        return await _dbContext.Database
            .SqlQueryRaw<OutboxClaimedMessage>(_claimSql, now, lockedUntil, workerId, batchSize)
            .ToListAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task CompleteBatchAsync(
        string workerId,
        IReadOnlyList<Guid> dispatchedMessageIds,
        IReadOnlyList<Guid> releaseMessageIds,
        CancellationToken cancellationToken = default)
    {
        if (releaseMessageIds.Count > 0)
        {
            await _dbContext.OutboxMessages
                .Where(message => releaseMessageIds.Contains(message.Id) && message.LockedBy == workerId)
                .ExecuteUpdateAsync(
                    setters => setters
                        .SetProperty(message => message.LockedUntil, (DateTimeOffset?)null)
                        .SetProperty(message => message.LockedBy, (string?)null),
                    cancellationToken);
        }

        if (dispatchedMessageIds.Count > 0)
        {
            await _dbContext.OutboxMessages
                .Where(message => dispatchedMessageIds.Contains(message.Id) && message.LockedBy == workerId)
                .ExecuteDeleteAsync(cancellationToken);
        }
    }

    private static string ResolveClaimSql(string? providerName) =>
        providerName switch
        {
            PostgreSqlProvider => PostgreSqlOutboxClaimSql.ClaimBatch,
            SqlServerProvider => SqlServerOutboxClaimSql.ClaimBatch,
            _ => throw new NotSupportedException(
                $"Outbox message leasing is not supported for database provider '{providerName}'. " +
                $"Supported providers: PostgreSQL ({PostgreSqlProvider}) and SQL Server ({SqlServerProvider})."),
        };
}
