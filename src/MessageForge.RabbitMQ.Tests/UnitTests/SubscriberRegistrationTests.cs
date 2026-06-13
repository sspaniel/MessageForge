using MessageForge.Errors;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class SubscriberRegistrationTests
{
    [Test]
    public void Subscribe_Registers_One_Entry_Per_Implemented_Interface()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act
        options.Subscribe<TestSubscriber>(subscriber => subscriber.MaxMessageConcurrency(7));

        // assert
        var registrations = options.SubscriberOptions
            .Where(o => o.SubscriberType == typeof(TestSubscriber))
            .ToList();

        registrations.Count.ShouldBe(4);

        registrations.Select(o => o.MessageType).ShouldBe(
            [
                typeof(TestSimpleMessage),
                typeof(TestComplexMessage),
                typeof(TestExceptionMessage),
                typeof(MessageForgeError),
            ],
            ignoreOrder: true);

        registrations.ShouldAllBe(o => o.MaxConcurrency == 7);
    }

    [Test]
    public void Subscribe_Throws_When_Type_Does_Not_Implement_ISubscriber()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act / assert
        Should.Throw<InvalidOperationException>(() =>
            options.Subscribe<object>(_ => { }));
    }

    [Test]
    public void AddSubscribersFromAssembly_Registers_All_Discovered_Subscribers()
    {
        // arrange
        var options = new MessageServiceOptions();
        var assembly = typeof(FanoutSubscriberA).Assembly;

        // act
        options.AddSubscribersFromAssembly(assembly, subscriber => subscriber.MaxMessageConcurrency(5));

        // assert
        options.SubscriberOptions.ShouldContain(o =>
            o.SubscriberType == typeof(FanoutSubscriberA) && o.MessageType == typeof(FanoutMessage));

        options.SubscriberOptions.ShouldContain(o =>
            o.SubscriberType == typeof(SerializerDeadLetterSubscriber) && o.MessageType == typeof(SerializerDeadLetterMessage));

        // a multi-interface subscriber contributes one registration per message type
        options.SubscriberOptions
            .Count(o => o.SubscriberType == typeof(TestSubscriber))
            .ShouldBe(4);

        options.SubscriberOptions.ShouldAllBe(o => o.MaxConcurrency == 5);
    }
}
