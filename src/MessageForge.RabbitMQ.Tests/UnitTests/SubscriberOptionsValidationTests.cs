using MessageForge.RabbitMQ.Subscribers;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class SubscriberOptionsValidationTests
{
    [Test]
    public void Validate_Does_Not_Throw_For_Default_Options()
    {
        // arrange
        var options = CreateValidOptions();

        // act / assert
        Should.NotThrow(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_SubscriberType_Is_Null()
    {
        // arrange
        var options = new SubscriberOptions(null!, typeof(TestSimpleMessage));

        // act / assert
        Should.Throw<ArgumentNullException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_Ttl_Is_Negative()
    {
        // arrange
        var options = CreateValidOptions();
        options.Ttl = TimeSpan.FromSeconds(-1);

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_MaxCount_Is_Negative()
    {
        // arrange
        var options = CreateValidOptions();
        options.MaxCount = -1;

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_MaxConcurrency_Is_Zero()
    {
        // arrange
        var options = CreateValidOptions();
        options.MaxConcurrency = 0;

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_MaxRetryCount_Is_Negative()
    {
        // arrange
        var options = CreateValidOptions();
        options.MaxRetryCount = -1;

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_RetryDelay_Is_Negative()
    {
        // arrange
        var options = CreateValidOptions();
        options.RetryDelay = TimeSpan.FromSeconds(-1);

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Throws_When_Retries_Enabled_But_RetryDelay_Is_Zero()
    {
        // arrange
        var options = CreateValidOptions();
        options.MaxRetryCount = 3;
        options.RetryDelay = TimeSpan.Zero;

        // act / assert
        Should.Throw<ArgumentOutOfRangeException>(() => options.Validate());
    }

    [Test]
    public void Validate_Does_Not_Throw_When_Retries_Disabled_And_RetryDelay_Is_Zero()
    {
        // arrange
        var options = CreateValidOptions();
        options.MaxRetryCount = 0;
        options.RetryDelay = TimeSpan.Zero;

        // act / assert
        Should.NotThrow(() => options.Validate());
    }

    private static SubscriberOptions CreateValidOptions() =>
        new(typeof(TestSubscriber), typeof(TestSimpleMessage));
}
