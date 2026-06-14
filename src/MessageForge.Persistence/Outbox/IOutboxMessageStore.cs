namespace MessageForge.Persistence.Outbox;

internal interface IOutboxMessageStore
{
    Task<IReadOnlyList<OutboxClaimedMessage>> ClaimBatchAsync(
        string workerId,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken cancellationToken = default);

    Task CompleteBatchAsync(
        string workerId,
        IReadOnlyList<Guid> dispatchedMessageIds,
        IReadOnlyList<Guid> releaseMessageIds,
        CancellationToken cancellationToken = default);
}
