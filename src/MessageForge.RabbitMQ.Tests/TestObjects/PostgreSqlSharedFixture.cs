using Npgsql;
using Testcontainers.PostgreSql;

namespace MessageForge.RabbitMQ.Tests.TestObjects;

/// <summary>
/// Starts a single PostgreSQL container shared by outbox integration tests.
/// </summary>
public static class PostgreSqlSharedFixture
{
    private static readonly SemaphoreSlim StartLock = new(1, 1);
    private static PostgreSqlContainer? _container;
    private static string _adminConnectionString = string.Empty;

    /// <summary>
    /// Creates an isolated database and returns a connection string for it.
    /// </summary>
    public static async Task<string> CreateDatabaseConnectionStringAsync()
    {
        await EnsureStartedAsync();

        var databaseName = $"outbox_{Guid.NewGuid():N}";

        await using var connection = new NpgsqlConnection(_adminConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $"CREATE DATABASE \"{databaseName}\"";
        await command.ExecuteNonQueryAsync();

        return new NpgsqlConnectionStringBuilder(_adminConnectionString)
        {
            Database = databaseName,
        }.ConnectionString;
    }

    private static async Task EnsureStartedAsync()
    {
        if (_container is not null)
        {
            return;
        }

        await StartLock.WaitAsync();

        try
        {
            if (_container is not null)
            {
                return;
            }

            _container = new PostgreSqlBuilder("postgres:16-alpine")
                .WithDatabase("postgres")
                .WithUsername("postgres")
                .WithPassword("postgres")
                .Build();

            await _container.StartAsync();
            _adminConnectionString = _container.GetConnectionString();
        }
        finally
        {
            StartLock.Release();
        }
    }
}
