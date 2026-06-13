using MessageForge.Errors;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using MessageForge.Subscribers;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

public sealed class PublishTests
{
    private IServiceScope _serviceScope = null!;
    private IPublisher _messagePublisher = null!;
    private MessageServiceOptions _messageOptions = null!;
    private MessageService _messageService = null!;
    private IConnectionPool _connectionPool = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        var serviceCollection = new ServiceCollection();

        serviceCollection
            .AddLogging()
            .AddMessageForgeRabbitMQ(options =>
            {
                options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);
                options.UseConnectionPoolSize(Environment.ProcessorCount);

                options.ConfigureMessagePublisher(publisherOptions =>
                {
                    publisherOptions.OnSerializationException(PublisherSerializerExceptionBehavior.Ignore);
                });

                options.Subscribe<TestSubscriber>(subscriberOptions =>
                {
                    subscriberOptions.MaxMessageConcurrency((ushort)Environment.ProcessorCount);
                    subscriberOptions.MessageTtl(TimeSpan.FromHours(1));
                    subscriberOptions.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(100));
                    subscriberOptions.OnSerializationException(SubscriberSerializerExceptionBehavior.DeadLetter);
                });
            });

        var serviceProvider = serviceCollection.BuildServiceProvider();

        _serviceScope = serviceProvider.CreateScope();
        _messagePublisher = _serviceScope.ServiceProvider.GetRequiredService<IPublisher>();
        _messageOptions = _serviceScope.ServiceProvider.GetRequiredService<MessageServiceOptions>();
        _connectionPool = _serviceScope.ServiceProvider.GetRequiredService<IConnectionPool>();
        _messageService = new MessageService(_serviceScope.ServiceProvider, _messageOptions, _connectionPool);
        await _messageService.StartAsync(CancellationToken.None);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        try
        {
            await _messageService.StopAsync(CancellationToken.None);
        }
        catch { }

        _connectionPool?.Dispose();
        _serviceScope?.Dispose();

        GC.SuppressFinalize(this);
    }

    [SetUp]
    public void TestSetUp()
    {
        TestSubscriber.ClearMessages();
    }

    private async Task WaitForMessagesAsync(Func<int> getMessageCount, int expectedCount, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(5);
        var stopWatch = System.Diagnostics.Stopwatch.StartNew();

        while (getMessageCount() < expectedCount && stopWatch.Elapsed < timeout)
        {
            await Task.Delay(50);
        }
    }

    [Test]
    public async Task SimpleMessages()
    {
        // arrange
        var publishedSimpleMessages = Enumerable.Range(0, 3)
            .Select(i => new TestSimpleMessage
            {
                Guid = Guid.NewGuid(),
                String = $"value-{i}",
                Integer = i,
                Float = i + 0.5f,
                DateTime = DateTime.UtcNow.AddMinutes(i),
            })
            .ToList();

        // act
        foreach (var publishedSimpleMessage in publishedSimpleMessages)
        {
            await _messagePublisher.PublishAsync(publishedSimpleMessage);
        }

        await WaitForMessagesAsync(() => TestSubscriber.SimpleMessages().Count(), publishedSimpleMessages.Count());

        // assert
        TestSubscriber.SimpleMessages().Count().ShouldBe(publishedSimpleMessages.Count());

        foreach (var publishedSimpleMessage in publishedSimpleMessages)
        {
            var receivedSimpleMessage = TestSubscriber.SimpleMessages()
                .SingleOrDefault(x => x.Guid == publishedSimpleMessage.Guid);

            receivedSimpleMessage.ShouldNotBeNull();
            receivedSimpleMessage.Guid.ShouldBe(publishedSimpleMessage.Guid);
            receivedSimpleMessage.String.ShouldBe(publishedSimpleMessage.String);
            receivedSimpleMessage.Integer.ShouldBe(publishedSimpleMessage.Integer);
            receivedSimpleMessage.Float.ShouldBe(publishedSimpleMessage.Float);
            receivedSimpleMessage.DateTime.ShouldBe(publishedSimpleMessage.DateTime);
        }
    }

    [Test]
    public async Task ComplexMessages()
    {
        // arrange
        var publishedComplexMessages = Enumerable.Range(0, 3)
            .Select(i => new TestComplexMessage
            {
                Guid = Guid.NewGuid(),
                SimpleMessages = Enumerable.Range(0, 3)
                    .Select(j => new TestSimpleMessage
                    {
                        Guid = Guid.NewGuid(),
                        String = $"complex-{i}-{j}",
                        Integer = j,
                        Float = j + 0.25f,
                        DateTime = DateTime.UtcNow.AddSeconds(j),
                    })
                    .ToList(),
            })
            .ToList();

        // act
        foreach (var publishedMessage in publishedComplexMessages)
        {
            await _messagePublisher.PublishAsync(publishedMessage);
        }

        await WaitForMessagesAsync(() => TestSubscriber.ComplexMessages().Count(), publishedComplexMessages.Count());

        // assert
        TestSubscriber.ComplexMessages().Count().ShouldBe(publishedComplexMessages.Count());

        foreach (var publishedComplexMessage in publishedComplexMessages)
        {
            var receivedComplexMessage = TestSubscriber.ComplexMessages()
                .SingleOrDefault(x => x.Guid == publishedComplexMessage.Guid);

            receivedComplexMessage.ShouldNotBeNull();
            receivedComplexMessage.Guid.ShouldBe(publishedComplexMessage.Guid);
            receivedComplexMessage.SimpleMessages.Count().ShouldBe(publishedComplexMessage.SimpleMessages.Count());

            foreach (var publishedSimpleMessage in publishedComplexMessage.SimpleMessages)
            {
                var receivedSimpleMessage = receivedComplexMessage.SimpleMessages
                    .SingleOrDefault(x => x.Guid == publishedSimpleMessage.Guid);

                receivedSimpleMessage.ShouldNotBeNull();
                receivedSimpleMessage.Guid.ShouldBe(publishedSimpleMessage.Guid);
                receivedSimpleMessage.String.ShouldBe(publishedSimpleMessage.String);
                receivedSimpleMessage.Integer.ShouldBe(publishedSimpleMessage.Integer);
                receivedSimpleMessage.Float.ShouldBe(publishedSimpleMessage.Float);
                receivedSimpleMessage.DateTime.ShouldBe(publishedSimpleMessage.DateTime);
            }
        }
    }

    [Test]
    public async Task Consumer_Exception_Thrown_Message_Is_Retried()
    {
        // arrange
        var publishedExceptionMessage = new TestExceptionMessage { Guid = Guid.NewGuid() };
        var expectedMessageCount = 3; // three retries

        // act
        await _messagePublisher.PublishAsync(publishedExceptionMessage);

        await WaitForMessagesAsync(() => TestSubscriber.ExceptionMessages().Count(), expectedMessageCount);

        // assert
        TestSubscriber.ExceptionMessages().Count().ShouldBe(expectedMessageCount);

        foreach (var receivedExceptionMessage in TestSubscriber.ExceptionMessages())
        {
            receivedExceptionMessage.ShouldNotBeNull();
            receivedExceptionMessage.Guid.ShouldBe(publishedExceptionMessage.Guid);
        }

        TestSubscriber.Errors().Count().ShouldBe(expectedMessageCount);

        foreach (var error in TestSubscriber.Errors())
        {
            error.ConsumerName.ShouldBe(typeof(TestSubscriber).Name);
            error.Message.ShouldBe("The method or operation is not implemented.");
            error.StackTrace.ShouldNotBeNullOrWhiteSpace();
        }
    }
}
