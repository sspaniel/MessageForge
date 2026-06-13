using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class PublisherFailureTests
{
    [Test]
    public async Task Publishing_To_Unreachable_Broker_Throws_MessagePublishException()
    {
        // arrange: point at a port where no broker is listening
        var services = new ServiceCollection();
        services
            .AddLogging()
            .AddMessageForgeRabbitMQ(options =>
                options.UseConnectionString("amqp://guest:guest@127.0.0.1:1/"));

        await using var provider = services.BuildServiceProvider();
        var publisher = provider.GetRequiredService<IPublisher>();

        try
        {
            // act / assert
            await Should.ThrowAsync<MessagePublishException>(
                async () => await publisher.PublishAsync(new TestSimpleMessage()));
        }
        finally
        {
            provider.GetRequiredService<IConnectionPool>().Dispose();
        }
    }
}
