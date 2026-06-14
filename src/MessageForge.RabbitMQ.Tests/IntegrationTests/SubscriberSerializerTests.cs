using System.Text;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using MessageForge.Subscribers;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class SubscriberSerializerTests
{
    private ServiceProvider _serviceProvider = null!;
    private MessageService _messageService = null!;
    private IConnectionPool _connectionPool = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        _serviceProvider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.Subscribe<SerializerDeadLetterSubscriber>(subscriber =>
            {
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50));
                subscriber.OnSerializationException(SubscriberSerializerExceptionBehavior.DeadLetter);
            });

            options.Subscribe<SerializerIgnoreSubscriber>(subscriber =>
            {
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50));
                subscriber.OnSerializationException(SubscriberSerializerExceptionBehavior.Ignore);
            });
        });

        _connectionPool = _serviceProvider.GetRequiredService<IConnectionPool>();
        var options = _serviceProvider.GetRequiredService<MessageServiceOptions>();
        _messageService = new MessageService(_serviceProvider, options, _connectionPool);
        await _messageService.StartAsync(CancellationToken.None);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        try
        {
            await _messageService.StopAsync(CancellationToken.None);
        }
        catch
        {
        }

        _connectionPool?.Dispose();
        await _serviceProvider.DisposeAsync();
    }

    [SetUp]
    public void TestSetUp()
    {
        SerializerDeadLetterSubscriber.Reset();
        SerializerIgnoreSubscriber.Reset();
    }

    [Test]
    public async Task Malformed_Payload_With_DeadLetter_Behavior_Is_Dead_Lettered()
    {
        // arrange
        var marker = Guid.NewGuid();
        var malformed = Encoding.UTF8.GetBytes($"this-is-not-valid-json-{marker}");
        var exchange = typeof(SerializerDeadLetterMessage).FullName!;

        // act
        await RabbitMqTestHelpers.PublishRawAsync(await _connectionPool.GetConnectionAsync(), exchange, malformed);

        var deadLettered = await RabbitMqTestHelpers.ReadDeadLetteredAsync(
            await _connectionPool.GetConnectionAsync(),
            body => body.Contains(marker.ToString(), StringComparison.OrdinalIgnoreCase),
            expectedCount: 1,
            timeout: TimeSpan.FromSeconds(15));

        // assert
        deadLettered.Count.ShouldBe(1);
        SerializerDeadLetterSubscriber.DeadLetterHandled.ShouldBe(0);
    }

    [Test]
    public async Task Malformed_Payload_With_Ignore_Behavior_Is_Dropped()
    {
        // arrange
        var marker = Guid.NewGuid();
        var malformed = Encoding.UTF8.GetBytes($"this-is-not-valid-json-{marker}");
        var exchange = typeof(SerializerIgnoreMessage).FullName!;

        // act
        await RabbitMqTestHelpers.PublishRawAsync(await _connectionPool.GetConnectionAsync(), exchange, malformed);

        // give the consumer time to ack-and-drop the message
        var deadLettered = await RabbitMqTestHelpers.ReadDeadLetteredAsync(
            await _connectionPool.GetConnectionAsync(),
            body => body.Contains(marker.ToString(), StringComparison.OrdinalIgnoreCase),
            expectedCount: 1,
            timeout: TimeSpan.FromSeconds(4));

        // assert
        deadLettered.Count.ShouldBe(0);
        SerializerIgnoreSubscriber.IgnoreHandled.ShouldBe(0);
    }
}
