using MessageForge.Persistence.Services;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Publishers;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Subscribers;
using MessageForge.RabbitMQ.Tests.TestObjects;
using MessageForge.Subscribers;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class OptionsConfigurationTests
{
    [Test]
    public void SubscriberOptions_Has_Expected_Defaults()
    {
        // act
        var options = CreateSubscriberOptions();

        // assert
        options.MaxConcurrency.ShouldBe((ushort)10);
        options.MaxRetryCount.ShouldBe(3);
        options.RetryDelay.ShouldBe(TimeSpan.FromSeconds(1));
        options.Ttl.ShouldBe(TimeSpan.Zero);
        options.MaxCount.ShouldBe(0);
    }

    [Test]
    public void SubscriberOptions_Fluent_Setters_Store_Values()
    {
        // arrange
        var options = CreateSubscriberOptions();

        // act
        options.MessageTtl(TimeSpan.FromMinutes(5));
        options.MaxMessageCount(250);
        options.MaxMessageConcurrency(20);
        options.Retries(7, TimeSpan.FromSeconds(2));
        options.OnSerializationException(SubscriberSerializerExceptionBehavior.DeadLetter);

        // assert
        options.Ttl.ShouldBe(TimeSpan.FromMinutes(5));
        options.MaxCount.ShouldBe(250);
        options.MaxConcurrency.ShouldBe((ushort)20);
        options.MaxRetryCount.ShouldBe(7);
        options.RetryDelay.ShouldBe(TimeSpan.FromSeconds(2));
        options.SerializerExceptionBehavior.ShouldBe(SubscriberSerializerExceptionBehavior.DeadLetter);
    }

    [Test]
    public void MessageServiceOptions_Has_Expected_Defaults()
    {
        // act
        var options = new MessageServiceOptions();

        // assert
        options.ConnectionString.ShouldBe(string.Empty);
        options.ConnectionPoolSize.ShouldBe(Environment.ProcessorCount);
        options.PublisherOptions.ShouldNotBeNull();
        options.SubscriberOptions.ShouldBeEmpty();
        options.IncludeMessageContentInTelemetry.ShouldBeFalse();
    }

    [Test]
    public void MessageServiceOptions_Setters_Store_Values()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act
        options.UseConnectionString("amqp://localhost");
        options.UseConnectionPoolSize(8);

        // assert
        options.ConnectionString.ShouldBe("amqp://localhost");
        options.ConnectionPoolSize.ShouldBe(8);
    }

    [Test]
    public void IncludeMessageContentInOpenTelemetry_Sets_Flag()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act
        options.IncludeMessageContentInOpenTelemetry();
        options.IncludeMessageContentInOpenTelemetry(false);

        // assert
        options.IncludeMessageContentInTelemetry.ShouldBeFalse();
    }

    [Test]
    public void IncludeMessageContentInOpenTelemetry_Enables_Message_Content_In_Telemetry()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act
        options.IncludeMessageContentInOpenTelemetry();

        // assert
        options.IncludeMessageContentInTelemetry.ShouldBeTrue();
    }

    [Test]
    public void ConfigureMessagePublisher_Mutates_PublisherOptions_Instance()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act
        options.ConfigureMessagePublisher(publisher =>
            publisher.OnSerializationException(PublisherSerializerExceptionBehavior.Throw));

        // assert
        options.PublisherOptions.SerializerExceptionBehavior.ShouldBe(PublisherSerializerExceptionBehavior.Throw);
    }

    [Test]
    public void AddSubscribersFromAssembly_Throws_When_Assembly_Is_Null()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act / assert
        Should.Throw<ArgumentNullException>(() =>
            options.AddSubscribersFromAssembly(null!, _ => { }));
    }

    [Test]
    public void AddSubscribersFromAssembly_Throws_When_Configure_Is_Null()
    {
        // arrange
        var options = new MessageServiceOptions();
        var assembly = typeof(TestSubscriber).Assembly;

        // act / assert
        Should.Throw<ArgumentNullException>(() =>
            options.AddSubscribersFromAssembly(assembly, null!));
    }

    [Test]
    public void PublisherOptions_OnSerializationException_Sets_Behavior_And_Returns_Self()
    {
        // arrange
        var options = new PublisherOptions();

        // act
        var returned = options.OnSerializationException(PublisherSerializerExceptionBehavior.Throw);

        // assert
        returned.ShouldBeSameAs(options);
        options.SerializerExceptionBehavior.ShouldBe(PublisherSerializerExceptionBehavior.Throw);
    }

    [Test]
    public void UseOutbox_Sets_DbContextType_And_Options()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act
        options.UseOutbox<TestOutboxDbContext>(outbox =>
        {
            outbox.PollingInterval = TimeSpan.FromSeconds(2);
            outbox.BatchSize = 25;
        });

        options.UseConnectionString("amqp://localhost");

        // assert
        options.OutboxOptions.ShouldNotBeNull();
        options.OutboxOptions!.PollingInterval.ShouldBe(TimeSpan.FromSeconds(2));
        options.OutboxOptions.BatchSize.ShouldBe(25);
        Should.NotThrow(() => options.Validate());
    }

    [Test]
    public void OutboxOptions_Has_Expected_Defaults()
    {
        // act
        var options = new OutboxOptions();

        // assert
        options.PollingInterval.ShouldBe(TimeSpan.FromSeconds(1));
        options.BatchSize.ShouldBe(100);
        options.RetentionPeriod.ShouldBe(TimeSpan.FromDays(30));
    }

    [Test]
    public void OutboxOptions_Fluent_Setters_Store_Values()
    {
        // arrange
        var options = new OutboxOptions();

        // act
        options
            .WithBatchSize(50)
            .WithPollingInterval(TimeSpan.FromSeconds(5))
            .WithDeduplication(false)
            .WithRetentionPeriod(TimeSpan.FromDays(7));

        // assert
        options.BatchSize.ShouldBe(50);
        options.PollingInterval.ShouldBe(TimeSpan.FromSeconds(5));
        options.RetentionPeriod.ShouldBe(TimeSpan.FromDays(7));
    }

    private static SubscriberOptions CreateSubscriberOptions() =>
        new(typeof(TestSubscriber), typeof(TestSimpleMessage));
}
