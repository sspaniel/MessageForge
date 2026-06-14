using System.Text;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class EdgeCaseTests
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
            options.Subscribe<NullMessageSubscriber>(subscriber =>
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
        NullMessageSubscriber.Reset();
    }

    [Test]
    public async Task Publishing_With_No_Bound_Subscriber_Does_Not_Throw()
    {
        // arrange: declare the exchange (no queue bound) so the unrouted message is silently dropped
        var exchange = typeof(NoSubscriberMessage).FullName!;
        await RabbitMqTestHelpers.DeclareFanoutExchangeAsync(await _connectionPool.GetConnectionAsync(), exchange);
        var message = new NoSubscriberMessage { Guid = Guid.NewGuid() };

        // act / assert
        await Should.NotThrowAsync(async () => await _publisher.PublishAsync(message));
    }

    [Test]
    public async Task Null_Deserialized_Message_Is_Acked_And_Dropped()
    {
        // arrange
        var exchange = typeof(NullableMessage).FullName!;
        var nullBody = Encoding.UTF8.GetBytes("null");

        // act
        await RabbitMqTestHelpers.PublishRawAsync(await _connectionPool.GetConnectionAsync(), exchange, nullBody);

        // allow the consumer to process and ack the null message
        await Task.Delay(TimeSpan.FromSeconds(2));

        // assert: nothing handled and nothing dead-lettered for this queue
        NullMessageSubscriber.Received.ShouldBe(0);
    }
}
