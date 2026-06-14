using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

internal static class OutboxIntegrationTestContext
{
    internal static async Task<(string ConnectionString, Action<DbContextOptionsBuilder, string> ConfigureDbContext)> CreateDatabaseAsync(
        OutboxDatabaseProvider provider)
    {
        return provider switch
        {
            OutboxDatabaseProvider.PostgreSql => (
                await PostgreSqlSharedFixture.CreateDatabaseConnectionStringAsync(),
                static (options, connection) => options.UseNpgsql(connection)),
            OutboxDatabaseProvider.SqlServer => (
                await SqlServerSharedFixture.CreateDatabaseConnectionStringAsync(),
                static (options, connection) => options.UseSqlServer(connection)),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, "Unsupported outbox database provider."),
        };
    }

    internal static async Task<(string ConnectionString, Action<DbContextOptionsBuilder, string> ConfigureDbContext)> CreateDatabaseWithRetryAsync(
        OutboxDatabaseProvider provider)
    {
        return provider switch
        {
            OutboxDatabaseProvider.PostgreSql => (
                await PostgreSqlSharedFixture.CreateDatabaseConnectionStringAsync(),
                static (options, connection) => options.UseNpgsql(connection, npgsql => npgsql.EnableRetryOnFailure())),
            OutboxDatabaseProvider.SqlServer => (
                await SqlServerSharedFixture.CreateDatabaseConnectionStringAsync(),
                static (options, connection) => options.UseSqlServer(connection, sql => sql.EnableRetryOnFailure())),
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, "Unsupported outbox database provider."),
        };
    }

    internal static async Task EnsureSchemaAsync(IServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
        await dbContext.Database.EnsureCreatedAsync();
    }
}
