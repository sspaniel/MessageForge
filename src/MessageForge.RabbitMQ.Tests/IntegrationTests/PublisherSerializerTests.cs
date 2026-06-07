using System.Text.Json;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class PublisherSerializerTests
{
    private ServiceProvider _throwProvider = null!;
    private ServiceProvider _ignoreProvider = null!;
    private IPublisher _throwPublisher = null!;
    private IPublisher _ignorePublisher = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _throwProvider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.ConfigureMessagePublisher(publisher =>
                publisher.OnSerializationException(PublisherSerializerExceptionBehavior.Throw));
        });

        _ignoreProvider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.ConfigureMessagePublisher(publisher =>
                publisher.OnSerializationException(PublisherSerializerExceptionBehavior.Ignore));
        });

        _throwPublisher = _throwProvider.GetRequiredService<IPublisher>();
        _ignorePublisher = _ignoreProvider.GetRequiredService<IPublisher>();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        _throwProvider.GetRequiredService<IConnectionPool>().Dispose();
        _ignoreProvider.GetRequiredService<IConnectionPool>().Dispose();
        await _throwProvider.DisposeAsync();
        await _ignoreProvider.DisposeAsync();
    }

    private static CyclicMessage CreateCyclicMessage()
    {
        var message = new CyclicMessage { Guid = Guid.NewGuid() };
        message.Self = message;
        return message;
    }

    [Test]
    public async Task Serialization_Failure_With_Throw_Behavior_Throws()
    {
        // arrange
        var message = CreateCyclicMessage();

        // act / assert
        await Should.ThrowAsync<JsonException>(async () => await _throwPublisher.PublishAsync(message));
    }

    [Test]
    public async Task Serialization_Failure_With_Ignore_Behavior_Does_Not_Throw()
    {
        // arrange
        var message = CreateCyclicMessage();

        // act / assert
        await Should.NotThrowAsync(async () => await _ignorePublisher.PublishAsync(message));
    }
}
