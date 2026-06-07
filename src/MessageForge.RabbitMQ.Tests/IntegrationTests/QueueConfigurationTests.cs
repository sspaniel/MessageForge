using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

/// <summary>
/// Verifies queue-level options (message TTL and max length). Both tests declare the queues but stop the
/// consumers so that messages remain enqueued long enough to be dead-lettered by the broker.
/// </summary>
public sealed class QueueConfigurationTests
{
    private const int MaxLength = 2;

    private ServiceProvider _serviceProvider = null!;
    private MessagingService _messagingService = null!;
    private IPublisher _publisher = null!;
    private IConnectionPool _connectionPool = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        _serviceProvider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.Subscribe<TtlSubscriber, TtlMessage>(subscriber =>
            {
                subscriber.MessageTtl(TimeSpan.FromSeconds(2));
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50));
            });

            options.Subscribe<MaxLengthSubscriber, MaxLengthMessage>(subscriber =>
            {
                subscriber.MaxMessageCount(MaxLength);
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50));
            });
        });

        _publisher = _serviceProvider.GetRequiredService<IPublisher>();
        _connectionPool = _serviceProvider.GetRequiredService<IConnectionPool>();
        var options = _serviceProvider.GetRequiredService<MessagingServiceOptions>();
        _messagingService = new MessagingService(_serviceProvider, options, _connectionPool);

        // Start to declare the topology, then stop so the queues retain messages without consuming them.
        await _messagingService.StartAsync(CancellationToken.None);
        await _messagingService.StopAsync(CancellationToken.None);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        _connectionPool?.Dispose();
        await _serviceProvider.DisposeAsync();
    }

    [SetUp]
    public void TestSetUp()
    {
        TtlSubscriber.Reset();
        MaxLengthSubscriber.Reset();
    }

    [Test]
    public async Task Expired_Message_Is_Dead_Lettered()
    {
        // arrange
        var message = new TtlMessage { Guid = Guid.NewGuid() };

        // act
        await _publisher.PublishAsync(message);

        var deadLettered = await RabbitMqTestHelpers.ReadDeadLetteredAsync(
            _connectionPool.GetConnection(),
            body => body.Contains(message.Guid.ToString(), StringComparison.OrdinalIgnoreCase),
            expectedCount: 1,
            timeout: TimeSpan.FromSeconds(20));

        // assert
        deadLettered.Count.ShouldBe(1);
        TtlSubscriber.Received.ShouldBe(0);
    }

    [Test]
    public async Task Overflowed_Messages_Are_Dead_Lettered()
    {
        // arrange
        var messages = Enumerable.Range(0, 5)
            .Select(_ => new MaxLengthMessage { Guid = Guid.NewGuid() })
            .ToList();
        var expectedOverflow = messages.Count - MaxLength;

        // act
        foreach (var message in messages)
        {
            await _publisher.PublishAsync(message);
        }

        var publishedGuids = messages.Select(m => m.Guid.ToString()).ToList();

        var deadLettered = await RabbitMqTestHelpers.ReadDeadLetteredAsync(
            _connectionPool.GetConnection(),
            body => publishedGuids.Any(guid => body.Contains(guid, StringComparison.OrdinalIgnoreCase)),
            expectedCount: expectedOverflow,
            timeout: TimeSpan.FromSeconds(15));

        // assert
        deadLettered.Count.ShouldBe(expectedOverflow);
        MaxLengthSubscriber.Received.ShouldBe(0);
    }
}
