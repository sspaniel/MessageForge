using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class LifecycleTests
{
    private ServiceProvider _serviceProvider = null!;
    private MessageServiceOptions _options = null!;
    private IPublisher _publisher = null!;
    private IConnectionPool _connectionPool = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _serviceProvider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.Subscribe<LifecycleSubscriber, LifecycleMessage>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        _publisher = _serviceProvider.GetRequiredService<IPublisher>();
        _connectionPool = _serviceProvider.GetRequiredService<IConnectionPool>();
        _options = _serviceProvider.GetRequiredService<MessageServiceOptions>();
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
        LifecycleSubscriber.Reset();
    }

    [Test]
    public async Task Message_Published_While_Stopped_Is_Delivered_After_Restart()
    {
        // arrange: declare topology then stop the consumer
        var initialService = new MessageService(_serviceProvider, _options, _connectionPool);
        await initialService.StartAsync(CancellationToken.None);
        await initialService.StopAsync(CancellationToken.None);

        var message = new LifecycleMessage { Guid = Guid.NewGuid() };

        // act: publish while no consumer is running, then start a fresh consumer
        await _publisher.PublishAsync(message);

        LifecycleSubscriber.Received.ShouldNotContain(message.Guid);

        var restartedService = new MessageService(_serviceProvider, _options, _connectionPool);
        await restartedService.StartAsync(CancellationToken.None);

        try
        {
            await RabbitMqTestHelpers.WaitForAsync(
                () => LifecycleSubscriber.Received.Contains(message.Guid),
                TimeSpan.FromSeconds(15));

            // assert
            LifecycleSubscriber.Received.ShouldContain(message.Guid);
        }
        finally
        {
            await restartedService.StopAsync(CancellationToken.None);
        }
    }
}
