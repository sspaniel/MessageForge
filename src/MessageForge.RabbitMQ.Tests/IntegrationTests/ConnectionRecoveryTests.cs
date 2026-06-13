using System.Diagnostics;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

/// <summary>
/// Verifies the connection pool's automatic recovery settings: after the broker becomes briefly unavailable
/// (simulated by pausing the shared container) the consumer recovers and resumes delivering messages.
/// </summary>
public sealed class ConnectionRecoveryTests
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
            options.Subscribe<RecoverySubscriber, RecoveryMessage>(subscriber =>
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
        RecoverySubscriber.Reset();
    }

    [Test]
    public async Task Consumer_Recovers_And_Delivers_After_Broker_Outage()
    {
        // arrange: confirm delivery works before the outage
        var beforeOutage = new RecoveryMessage { Guid = Guid.NewGuid() };
        await _publisher.PublishAsync(beforeOutage);
        (await RabbitMqTestHelpers.WaitForAsync(
            () => RecoverySubscriber.Received.Contains(beforeOutage.Guid),
            TimeSpan.FromSeconds(15))).ShouldBeTrue();

        // act: pause the shared broker long enough to drop the connection, then resume
        var delivered = false;
        await RabbitMqSharedFixture.PauseAsync();

        try
        {
            await Task.Delay(TimeSpan.FromSeconds(15));
        }
        finally
        {
            await RabbitMqSharedFixture.UnpauseAsync();
        }

        // assert: after recovery, a freshly published message is delivered (retry publishing through the recovery window)
        var afterOutage = new RecoveryMessage { Guid = Guid.NewGuid() };
        var stopwatch = Stopwatch.StartNew();

        while (!delivered && stopwatch.Elapsed < TimeSpan.FromSeconds(60))
        {
            try
            {
                await _publisher.PublishAsync(afterOutage);
            }
            catch
            {
                // connection may still be recovering; retry shortly
            }

            delivered = await RabbitMqTestHelpers.WaitForAsync(
                () => RecoverySubscriber.Received.Contains(afterOutage.Guid),
                TimeSpan.FromSeconds(3));
        }

        delivered.ShouldBeTrue();
    }
}
