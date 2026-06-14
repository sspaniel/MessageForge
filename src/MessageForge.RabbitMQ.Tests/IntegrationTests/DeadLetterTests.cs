using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using MessageForge.Subscribers;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class DeadLetterTests
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
            options.Subscribe<DeadLetterSubscriber>(subscriber =>
            {
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50));
                subscriber.OnSerializationException(SubscriberSerializerExceptionBehavior.DeadLetter);
            });

            options.Subscribe<ImmediateRejectSubscriber>(subscriber =>
            {
                subscriber.Retries(maxRetryCount: 0, retryDelay: TimeSpan.Zero);
                subscriber.OnSerializationException(SubscriberSerializerExceptionBehavior.DeadLetter);
            });
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
        DeadLetterSubscriber.Reset();
        ImmediateRejectSubscriber.Reset();
    }

    [Test]
    public async Task Retries_Exhausted_Message_Is_Dead_Lettered()
    {
        // arrange
        var message = new DeadLetterMessage { Guid = Guid.NewGuid() };

        // act
        await _publisher.PublishAsync(message);

        var deadLettered = await RabbitMqTestHelpers.ReadDeadLetteredAsync(
            await _connectionPool.GetConnectionAsync(),
            body => body.Contains(message.Guid.ToString(), StringComparison.OrdinalIgnoreCase),
            expectedCount: 1,
            timeout: TimeSpan.FromSeconds(15));

        // assert
        deadLettered.Count.ShouldBe(1);
        DeadLetterSubscriber.Attempts.ShouldBe(3);
    }

    [Test]
    public async Task MaxRetryCount_Zero_Message_Is_Immediately_Dead_Lettered()
    {
        // arrange
        var message = new ImmediateRejectMessage { Guid = Guid.NewGuid() };

        // act
        await _publisher.PublishAsync(message);

        var deadLettered = await RabbitMqTestHelpers.ReadDeadLetteredAsync(
            await _connectionPool.GetConnectionAsync(),
            body => body.Contains(message.Guid.ToString(), StringComparison.OrdinalIgnoreCase),
            expectedCount: 1,
            timeout: TimeSpan.FromSeconds(15));

        // assert
        deadLettered.Count.ShouldBe(1);
        ImmediateRejectSubscriber.Attempts.ShouldBe(0);
    }
}
