using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class MessageServiceOptionsValidationTests
{
    [Test]
    public void Validate_Does_Not_Throw_For_Valid_Options()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString("amqp://localhost");
        options.Subscribe<TestSubscriber>(subscriber => subscriber.MaxMessageConcurrency(5));

        // act / assert
        Should.NotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_ConnectionString_Not_Set()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act / assert
        Should.Throw<ArgumentNullException>(() => options.Validate());
    }

    [TestCase("")]
    [TestCase(" ")]
    public void Validate_Throws_When_ConnectionString_Is_Blank(string connectionString)
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString(connectionString);

        // act / assert
        Should.Throw<ArgumentNullException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_ConnectionPoolSize_Is_Less_Than_One()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString("amqp://localhost");
        options.UseConnectionPoolSize(0);

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Cascades_To_Subscriber_Validation()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString("amqp://localhost");
        options.Subscribe<TestSubscriber>(subscriber => subscriber.MaxMessageConcurrency(0));

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_Outbox_Enabled_Without_DbContextType()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString("amqp://localhost");
        options.ConfigureOutbox(_ => { });

        // act / assert
        Should.Throw<InvalidOperationException>(() => options.Validate());
    }

    [Test]
    public void Validate_Does_Not_Throw_When_Outbox_Configured()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString("amqp://localhost");
        options.UseOutbox<TestOutboxDbContext>();

        // act / assert
        Should.NotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_Outbox_BatchSize_Is_Invalid()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString("amqp://localhost");
        options.UseOutbox<TestOutboxDbContext>(outbox => outbox.WithBatchSize(0));

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_Outbox_RetentionPeriod_Is_Invalid()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString("amqp://localhost");
        options.UseOutbox<TestOutboxDbContext>(outbox => outbox.WithRetentionPeriod(TimeSpan.Zero));

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_Outbox_PurgeInterval_Is_Invalid()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.UseConnectionString("amqp://localhost");
        options.UseOutbox<TestOutboxDbContext>(outbox => outbox.WithPurgeInterval(TimeSpan.Zero));

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }
}
