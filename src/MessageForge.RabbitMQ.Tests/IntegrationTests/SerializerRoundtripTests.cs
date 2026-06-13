using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

/// <summary>
/// Round-trips messages with optional/null fields, enums and collections to verify the serializer's
/// <c>WhenWritingNull</c> behavior survives publish and subscribe.
/// </summary>
public sealed class SerializerRoundtripTests
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
            options.Subscribe<SerializerRoundtripSubscriber, OptionalFieldsMessage>(subscriber =>
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
        SerializerRoundtripSubscriber.Reset();
    }

    private async Task<OptionalFieldsMessage> PublishAndReceiveAsync(OptionalFieldsMessage message)
    {
        await _publisher.PublishAsync(message);

        await RabbitMqTestHelpers.WaitForAsync(
            () => SerializerRoundtripSubscriber.Received.Any(m => m.Guid == message.Guid),
            TimeSpan.FromSeconds(15));

        var received = SerializerRoundtripSubscriber.Received.SingleOrDefault(m => m.Guid == message.Guid);
        received.ShouldNotBeNull();
        return received;
    }

    [Test]
    public async Task Null_Optional_Fields_Round_Trip_As_Null()
    {
        // arrange
        var message = new OptionalFieldsMessage
        {
            Guid = Guid.NewGuid(),
            OptionalString = null,
            OptionalNumber = null,
            Status = SampleStatus.Archived,
            Counts = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 },
            Items = ["x", null, "y"],
        };

        // act
        var received = await PublishAndReceiveAsync(message);

        // assert
        received.OptionalString.ShouldBeNull();
        received.OptionalNumber.ShouldBeNull();
        received.Status.ShouldBe(SampleStatus.Archived);
        received.Counts.ShouldBe(message.Counts);
        received.Items.ShouldBe(message.Items);
    }

    [Test]
    public async Task Populated_Fields_Round_Trip_Unchanged()
    {
        // arrange
        var message = new OptionalFieldsMessage
        {
            Guid = Guid.NewGuid(),
            OptionalString = "present",
            OptionalNumber = 42,
            Status = SampleStatus.Active,
            Counts = new Dictionary<string, int> { ["only"] = 7 },
            Items = ["one", "two"],
        };

        // act
        var received = await PublishAndReceiveAsync(message);

        // assert
        received.OptionalString.ShouldBe("present");
        received.OptionalNumber.ShouldBe(42);
        received.Status.ShouldBe(SampleStatus.Active);
        received.Counts.ShouldBe(message.Counts);
        received.Items.ShouldBe(message.Items);
    }
}
