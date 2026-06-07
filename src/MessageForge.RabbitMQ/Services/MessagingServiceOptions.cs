using MessageForge.RabbitMQ.Publishers;
using MessageForge.RabbitMQ.Subscribers;
using MessageForge.Subscribers;

namespace MessageForge.RabbitMQ.Services;

/// <summary>
/// Options for RabbitMq messaging.
/// </summary>
public sealed class MessagingServiceOptions
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
    /// Adds a message subscriber to the service collection.
    /// </summary>
    /// <typeparam name="TSubscriber">Type of subscriber.</typeparam>
    /// <typeparam name="TMessage">Type of message.</typeparam>
    /// <param name="configure">Action to configure the subscriber options.</param>
    public void Subscribe<TSubscriber, TMessage>(Action<SubscriberOptions> configure)
        where TSubscriber : class, ISubscriber<TMessage>
        where TMessage : new()
    {
        var options = new SubscriberOptions(typeof(TSubscriber), typeof(TMessage));
        configure(options);

        SubscriberOptions.Add(options);
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
}
