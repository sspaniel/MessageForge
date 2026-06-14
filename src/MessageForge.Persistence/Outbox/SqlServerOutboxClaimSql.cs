namespace MessageForge.Persistence.Outbox;

internal static class SqlServerOutboxClaimSql
{
    internal const string ClaimBatch = """
        UPDATE m
        SET LockedUntil = {1}, LockedBy = {2}
        OUTPUT inserted.Id, inserted.MessageType, inserted.Payload
        FROM MessageForge.OutboxMessages AS m
        INNER JOIN (
            SELECT TOP ({3}) Id
            FROM MessageForge.OutboxMessages WITH (ROWLOCK, READPAST, UPDLOCK)
            WHERE LockedUntil IS NULL OR LockedUntil < {0}
            ORDER BY Sequence
        ) AS c ON m.Id = c.Id
        """;
}
