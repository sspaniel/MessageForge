namespace MessageForge.Persistence.Outbox;

internal static class PostgreSqlOutboxClaimSql
{
    internal const string ClaimBatch = """
        UPDATE "MessageForge"."OutboxMessages" AS m
        SET "LockedUntil" = {1}, "LockedBy" = {2}
        FROM (
            SELECT "Id"
            FROM "MessageForge"."OutboxMessages"
            WHERE ("LockedUntil" IS NULL OR "LockedUntil" < {0})
            ORDER BY "Sequence"
            LIMIT {3}
            FOR UPDATE SKIP LOCKED
        ) AS c
        WHERE m."Id" = c."Id"
        RETURNING m."Id", m."MessageType", m."Payload"
        """;
}
