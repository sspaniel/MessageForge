using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace MessageForge.RabbitMQ.Tests.TestObjects;

/// <summary>
/// Starts a single SQL Server container shared by outbox integration tests.
/// </summary>
public static class SqlServerSharedFixture
{
    private static readonly SemaphoreSlim StartLock = new(1, 1);
    private static MsSqlContainer? _container;
    private static string _adminConnectionString = string.Empty;

    /// <summary>
    /// Creates an isolated database and returns a connection string for it.
    /// </summary>
    public static async Task<string> CreateDatabaseConnectionStringAsync()
    {
        await EnsureStartedAsync();

        var databaseName = $"outbox_{Guid.NewGuid():N}";

        await using var connection = new SqlConnection(_adminConnectionString);
        await connection.OpenAsync();

        await using var command = connection.CreateCommand();
        command.CommandText = $"CREATE DATABASE [{databaseName}]";
        await command.ExecuteNonQueryAsync();

        var builder = new SqlConnectionStringBuilder(_adminConnectionString)
        {
            InitialCatalog = databaseName,
        };

        return builder.ConnectionString;
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

            _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
                .WithPassword("yourStrong(!)Password")
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
