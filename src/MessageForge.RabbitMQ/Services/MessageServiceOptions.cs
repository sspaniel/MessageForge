using System.Reflection;
using MessageForge.RabbitMQ.Publishers;
using MessageForge.RabbitMQ.Subscribers;
using MessageForge.Subscribers;

namespace MessageForge.RabbitMQ.Services;

/// <summary>
/// Options for RabbitMq messaging.
/// </summary>
public sealed class MessageServiceOptions
{
    internal string ConnectionString { get; set; } = string.Empty;

    internal int ConnectionPoolSize { get; set; } = Environment.ProcessorCount;

    internal PublisherOptions PublisherOptions { get; set; } = new PublisherOptions();

    internal ICollection<SubscriberOptions> SubscriberOptions { get; set; } = new LinkedList<SubscriberOptions>();

    /// <summary>
    /// Sets the connection string.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    public void UseConnectionString(string connectionString)
    {
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Sets the connection pool size.
    /// </summary>
    /// <param name="connectionPoolSize">Maximum number of connections.</param>
    public void UseConnectionPoolSize(int connectionPoolSize)
    {
        ConnectionPoolSize = connectionPoolSize;
    }

    /// <summary>
    /// Adds a message publisher to the service collection.
    /// </summary>
    /// <param name="configure">Action to configure the publisher options.</param>
    public void ConfigureMessagePublisher(Action<PublisherOptions> configure)
    {
        configure(PublisherOptions);
    }

    /// <summary>
    /// Adds a message subscriber to the service collection. The subscriber is registered for every
    /// <see cref="ISubscriber{TMessage}"/> interface it implements, applying the same configured options to each.
    /// </summary>
    /// <typeparam name="TSubscriber">Type of subscriber.</typeparam>
    /// <param name="configure">Action to configure the subscriber options.</param>
    public void Subscribe<TSubscriber>(Action<SubscriberOptions> configure)
        where TSubscriber : class
    {
        AddSubscriber(typeof(TSubscriber), configure);
    }

    /// <summary>
    /// Scans the supplied assembly for every concrete <see cref="ISubscriber{TMessage}"/> implementation and registers
    /// each one, applying the same configured options to every message type's registration.
    /// </summary>
    /// <param name="assembly">The assembly to scan for subscribers.</param>
    /// <param name="configure">Action to configure the subscriber options.</param>
    public void AddSubscribersFromAssembly(Assembly assembly, Action<SubscriberOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        ArgumentNullException.ThrowIfNull(configure);

        var subscriberTypes = assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract)
            .Where(t => GetMessageTypes(t).Any());

        foreach (var subscriberType in subscriberTypes)
        {
            AddSubscriber(subscriberType, configure);
        }
    }

    /// <summary>
    /// Validates the messaging options.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
        {
            throw new ArgumentNullException(nameof(ConnectionString));
        }

        if (PublisherOptions == null)
        {
            throw new ArgumentNullException(nameof(PublisherOptions));
        }

        if (ConnectionPoolSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(ConnectionPoolSize));
        }

        PublisherOptions.Validate();

        foreach (var subscriberOptions in SubscriberOptions)
        {
            if (subscriberOptions == null)
            {
                throw new ArgumentNullException(nameof(subscriberOptions));
            }

            subscriberOptions.Validate();
        }
    }

    private static IEnumerable<Type> GetMessageTypes(Type subscriberType) =>
        subscriberType.GetInterfaces()
            .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ISubscriber<>))
            .Select(i => i.GenericTypeArguments[0]);

    private void AddSubscriber(Type subscriberType, Action<SubscriberOptions> configure)
    {
        var messageTypes = GetMessageTypes(subscriberType).ToList();
        if (messageTypes.Count == 0)
        {
            throw new InvalidOperationException(
                $"{subscriberType.FullName} does not implement ISubscriber<>.");
        }

        foreach (var messageType in messageTypes)
        {
            var options = new SubscriberOptions(subscriberType, messageType);
            configure(options);

            SubscriberOptions.Add(options);
        }
    }
}
