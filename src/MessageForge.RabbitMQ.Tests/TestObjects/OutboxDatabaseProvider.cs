namespace MessageForge.RabbitMQ.Tests.TestObjects;

/// <summary>
/// Relational database providers used by outbox integration tests.
/// </summary>
public enum OutboxDatabaseProvider
{
    /// <summary>
    /// PostgreSQL via Npgsql.
    /// </summary>
    PostgreSql,

    /// <summary>
    /// Microsoft SQL Server.
    /// </summary>
    SqlServer,
}
