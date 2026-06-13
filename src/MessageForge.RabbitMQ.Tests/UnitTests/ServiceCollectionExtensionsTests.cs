using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Publishers;
using MessageForge.RabbitMQ.Serializers;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class ServiceCollectionExtensionsTests
{
    [Test]
    public void AddMessageForgeRabbitMQ_Registers_Core_Services_As_Singletons()
    {
        // arrange
        var services = new ServiceCollection();

        // act
        services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString("amqp://localhost");
            options.Subscribe<TestSubscriber>(subscriber => subscriber.MaxMessageConcurrency(5));
        });

        // assert
        ShouldBeRegistered(services, typeof(MessageServiceOptions), ServiceLifetime.Singleton);
        ShouldBeRegistered(services, typeof(IConnectionPool), ServiceLifetime.Singleton, typeof(ConnectionPool));
        ShouldBeRegistered(services, typeof(IMessageSerializer), ServiceLifetime.Singleton, typeof(MessageSerializer));
        ShouldBeRegistered(services, typeof(IPublisher), ServiceLifetime.Singleton, typeof(Publisher));
    }

    [Test]
    public void AddMessageForgeRabbitMQ_Registers_Subscriber_Type_As_Scoped()
    {
        // arrange
        var services = new ServiceCollection();

        // act
        services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString("amqp://localhost");
            options.Subscribe<TestSubscriber>(_ => { });
        });

        // assert
        var subscriberDescriptors = services
            .Where(d => d.ServiceType == typeof(TestSubscriber))
            .ToList();

        subscriberDescriptors.ShouldNotBeEmpty();
        subscriberDescriptors.ShouldAllBe(d => d.Lifetime == ServiceLifetime.Scoped);
    }

    [Test]
    public void AddMessageForgeRabbitMQ_Registers_Hosted_Service_When_Subscribers_Exist()
    {
        // arrange
        var services = new ServiceCollection();

        // act
        services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString("amqp://localhost");
            options.Subscribe<TestSubscriber>(_ => { });
        });

        // assert
        services.ShouldContain(d =>
            d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(MessageService));
    }

    [Test]
    public void AddMessageForgeRabbitMQ_Does_Not_Register_Hosted_Service_When_No_Subscribers()
    {
        // arrange
        var services = new ServiceCollection();

        // act
        services.AddMessageForgeRabbitMQ(options =>
            options.UseConnectionString("amqp://localhost"));

        // assert
        services.ShouldNotContain(d =>
            d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(MessageService));
    }

    [Test]
    public void AddMessageForgeRabbitMQ_Throws_For_Invalid_Configuration()
    {
        // arrange
        var services = new ServiceCollection();

        // act / assert
        Should.Throw<ArgumentNullException>(() =>
            services.AddMessageForgeRabbitMQ(_ => { }));
    }

    private static void ShouldBeRegistered(
        IServiceCollection services,
        Type serviceType,
        ServiceLifetime lifetime,
        Type? implementationType = null)
    {
        var descriptor = services.SingleOrDefault(d => d.ServiceType == serviceType);
        descriptor.ShouldNotBeNull();
        descriptor!.Lifetime.ShouldBe(lifetime);

        if (implementationType != null)
        {
            descriptor.ImplementationType.ShouldBe(implementationType);
        }
    }
}
