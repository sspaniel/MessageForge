using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Lifecycle;
using MessageForge.RabbitMQ.Publishers;
using MessageForge.RabbitMQ.Serializers;
using MessageForge.RabbitMQ.Services;
using Microsoft.Extensions.DependencyInjection;

namespace MessageForge.RabbitMQ.DependencyInjection;

/// <summary>
/// Dependency injection extensions for RabbitMq messaging.
/// </summary>
public static class Extensions
{
    /// <summary>
    /// Adds MessageForge RabbitMq messaging to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Action to configure <see cref="MessageServiceOptions"/>.</param>
    public static void AddMessageForgeRabbitMQ(this IServiceCollection services, Action<MessageServiceOptions> configure)
    {
        var options = new MessageServiceOptions();
        configure(options);
        LifecycleLoggingHooks.Register(options);
        options.Validate();

        services.AddSingleton(options);
        services.AddSingleton<IConnectionPool, ConnectionPool>();
        services.AddSingleton<IMessageSerializer, MessageSerializer>();
        services.AddSingleton<IPublisher, Publisher>();

        foreach (var subscriberOptions in options.SubscriberOptions)
        {
            services.AddScoped(subscriberOptions.SubscriberType);
        }

        if (options.SubscriberOptions.Any())
        {
            services.AddHostedService<MessageService>();
        }
    }
}
