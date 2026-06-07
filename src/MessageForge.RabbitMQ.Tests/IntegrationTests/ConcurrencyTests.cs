using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class ConcurrencyTests
{
    private const ushort MaxConcurrency = 5;
    private const int MessageCount = 30;

    private ServiceProvider _serviceProvider = null!;
    private MessagingService _messagingService = null!;
    private IPublisher _publisher = null!;
    private IConnectionPool _connectionPool = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        _serviceProvider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.Subscribe<ConcurrencySubscriber, ConcurrencyMessage>(subscriber =>
            {
                subscriber.MaxMessageConcurrency(MaxConcurrency);
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50));
            });
        });

        _publisher = _serviceProvider.GetRequiredService<IPublisher>();
        _connectionPool = _serviceProvider.GetRequiredService<IConnectionPool>();
        var options = _serviceProvider.GetRequiredService<MessagingServiceOptions>();
        _messagingService = new MessagingService(_serviceProvider, options, _connectionPool);
        await _messagingService.StartAsync(CancellationToken.None);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        try
        {
            await _messagingService.StopAsync(CancellationToken.None);
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
        ConcurrencySubscriber.Reset();
    }

    [Test]
    public async Task All_Messages_Are_Drained_Within_Prefetch_Limit()
    {
        // arrange
        var messages = Enumerable.Range(0, MessageCount)
            .Select(_ => new ConcurrencyMessage { Guid = Guid.NewGuid() })
            .ToList();

        // act
        foreach (var message in messages)
        {
            await _publisher.PublishAsync(message);
        }

        await RabbitMqTestHelpers.WaitForAsync(
            () => ConcurrencySubscriber.Received.Count >= MessageCount,
            TimeSpan.FromSeconds(30));

        // assert: every message is delivered, handlers run in parallel, and dispatch never exceeds the prefetch limit
        ConcurrencySubscriber.Received.Count.ShouldBe(MessageCount);
        ConcurrencySubscriber.MaxObservedConcurrency.ShouldBeGreaterThan(1);
        ConcurrencySubscriber.MaxObservedConcurrency.ShouldBeLessThanOrEqualTo(MaxConcurrency);
    }
}
