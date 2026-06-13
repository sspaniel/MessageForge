using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class FanoutTests
{
    private ServiceProvider _serviceProvider = null!;
    private MessageService _messageService = null!;
    private IPublisher _publisher = null!;
    private IConnectionPool _connectionPool = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        _serviceProvider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.Subscribe<FanoutSubscriberA>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));

            options.Subscribe<FanoutSubscriberB>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        _publisher = _serviceProvider.GetRequiredService<IPublisher>();
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
        FanoutSubscriberA.Reset();
        FanoutSubscriberB.Reset();
    }

    [Test]
    public async Task Message_Is_Broadcast_To_All_Subscribers()
    {
        // arrange
        var messages = Enumerable.Range(0, 3)
            .Select(_ => new FanoutMessage { Guid = Guid.NewGuid() })
            .ToList();

        // act
        foreach (var message in messages)
        {
            await _publisher.PublishAsync(message);
        }

        await RabbitMqTestHelpers.WaitForAsync(
            () => FanoutSubscriberA.Received.Count >= messages.Count && FanoutSubscriberB.Received.Count >= messages.Count,
            TimeSpan.FromSeconds(15));

        // assert
        foreach (var message in messages)
        {
            FanoutSubscriberA.Received.ShouldContain(message.Guid);
            FanoutSubscriberB.Received.ShouldContain(message.Guid);
        }
    }
}
