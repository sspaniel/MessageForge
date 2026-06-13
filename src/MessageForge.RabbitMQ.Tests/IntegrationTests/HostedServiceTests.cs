using MessageForge.Publishers;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

/// <summary>
/// Exercises the real production wiring: registration through <c>AddMessageForgeRabbitMq</c> and startup of the
/// hosted <c>MessageService</c> through an <see cref="IHost"/> (rather than constructing it manually).
/// </summary>
public sealed class HostedServiceTests
{
    [SetUp]
    public void TestSetUp()
    {
        HostedSubscriber.Reset();
    }

    [Test]
    public async Task Host_Starts_Hosted_Service_And_Delivers_Messages()
    {
        // arrange
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);
            options.Subscribe<HostedSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        using var host = builder.Build();

        // act
        await host.StartAsync();

        try
        {
            var publisher = host.Services.GetRequiredService<IPublisher>();
            var message = new HostedMessage { Guid = Guid.NewGuid() };
            await publisher.PublishAsync(message);

            await RabbitMqTestHelpers.WaitForAsync(
                () => HostedSubscriber.Received.Contains(message.Guid),
                TimeSpan.FromSeconds(15));

            // assert
            HostedSubscriber.Received.ShouldContain(message.Guid);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Test]
    public void Hosted_Service_Is_Registered_When_Subscribers_Exist()
    {
        // arrange
        using var provider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.Subscribe<HostedSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        // act
        var hostedServices = provider.GetServices<IHostedService>();

        // assert
        hostedServices.ShouldContain(service => service is MessageService);
    }

    [Test]
    public void Hosted_Service_Is_Not_Registered_When_No_Subscribers()
    {
        // arrange
        using var provider = RabbitMqTestHelpers.BuildServiceProvider(_ => { });

        // act
        var hostedServices = provider.GetServices<IHostedService>();

        // assert
        hostedServices.ShouldNotContain(service => service is MessageService);
    }
}
