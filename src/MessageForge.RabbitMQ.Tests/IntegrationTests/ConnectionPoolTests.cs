using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using RabbitMQ.Client;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class ConnectionPoolTests
{
    private static MessagingServiceOptions BuildOptions(int poolSize)
    {
        var options = new MessagingServiceOptions();
        options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);
        options.UseConnectionPoolSize(poolSize);
        return options;
    }

    [Test]
    public void Pool_Reuses_Connections_Up_To_The_Configured_Size()
    {
        // arrange
        const int poolSize = 2;
        var pool = new ConnectionPool(BuildOptions(poolSize));

        try
        {
            // act
            var distinctConnections = new HashSet<IConnection>();
            for (var i = 0; i < poolSize * 3; i++)
            {
                distinctConnections.Add(pool.GetConnection());
            }

            // assert
            distinctConnections.Count.ShouldBe(poolSize);
        }
        finally
        {
            pool.Dispose();
        }
    }

    [Test]
    public async Task Pool_Replaces_A_Closed_Connection()
    {
        // arrange
        var pool = new ConnectionPool(BuildOptions(1));

        try
        {
            var first = pool.GetConnection();
            await first.CloseAsync();
            first.IsOpen.ShouldBeFalse();

            // act
            var second = pool.GetConnection();

            // assert
            second.IsOpen.ShouldBeTrue();
            second.ShouldNotBeSameAs(first);
        }
        finally
        {
            pool.Dispose();
        }
    }

    [Test]
    public void Dispose_Closes_All_Pooled_Connections()
    {
        // arrange
        var pool = new ConnectionPool(BuildOptions(2));
        var first = pool.GetConnection();
        var second = pool.GetConnection();
        first.ShouldNotBeSameAs(second);

        // act
        pool.Dispose();

        // assert
        first.IsOpen.ShouldBeFalse();
        second.IsOpen.ShouldBeFalse();
    }
}
