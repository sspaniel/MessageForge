using System.Reflection;
using MessageForge.Persistence.Services;
using MessageForge.RabbitMQ.Lifecycle;
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

    internal OutboxOptions? OutboxOptions { get; set; }

    internal ICollection<SubscriberOptions> SubscriberOptions { get; set; } = new LinkedList<SubscriberOptions>();

    internal LinkedList<Func<MessageServiceContext, Task>> BeforeMessageServiceStartHooks { get; } = new();

    internal LinkedList<Func<MessageServiceContext, Task>> AfterMessageServiceStartedHooks { get; } = new();

    internal LinkedList<Func<MessagePublishContext, Task>> BeforeMessagePublishHooks { get; } = new();

    internal LinkedList<Func<MessagePublishContext, Task>> AfterMessagePublishedHooks { get; } = new();

    internal LinkedList<Func<MessageHandleContext, Task>> BeforeMessageHandleHooks { get; } = new();

    internal LinkedList<Func<MessageHandleContext, Task>> AfterMessageHandledHooks { get; } = new();

    internal LinkedList<Func<MessageServiceContext, Task>> BeforeMessageServiceStopHooks { get; } = new();

    internal LinkedList<Func<MessageServiceContext, Task>> AfterMessageServiceStoppedHooks { get; } = new();

    internal LinkedList<Func<MessageErrorContext, Task>> OnMessageHandleErrorHooks { get; } = new();

    internal LinkedList<Func<MessageErrorContext, Task>> OnMessagePublishErrorHooks { get; } = new();

    internal LinkedList<Func<MessageErrorContext, Task>> OnMessageDeserializeErrorHooks { get; } = new();

    internal LinkedList<Func<MessageErrorContext, Task>> OnMessageSerializeErrorHooks { get; } = new();

    internal LinkedList<Func<MessageErrorContext, Task>> OnMessageRetryHooks { get; } = new();

    internal LinkedList<Func<MessageErrorContext, Task>> OnRetryLimitReachedHooks { get; } = new();

    internal bool IncludeMessageContentInTelemetry { get; set; }

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
    /// Configures whether serialized message content is included in OpenTelemetry span tags.
    /// Disabled by default.
    /// </summary>
    /// <param name="include">Whether to include message content in telemetry.</param>
    public void IncludeMessageContentInOpenTelemetry(bool include = true)
    {
        IncludeMessageContentInTelemetry = include;
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
    /// Enables and configures the transactional outbox.
    /// </summary>
    /// <param name="configure">Action to configure the outbox options.</param>
    public void ConfigureOutbox(Action<OutboxOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        OutboxOptions ??= new OutboxOptions();
        configure(OutboxOptions);
    }

    /// <summary>
    /// Registers a hook invoked before the message service starts.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void BeforeMessageServiceStart(Func<MessageServiceContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        BeforeMessageServiceStartHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked after the message service has started.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void AfterMessageServiceStarted(Func<MessageServiceContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        AfterMessageServiceStartedHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked before a message is published.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void BeforeMessagePublish(Func<MessagePublishContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        BeforeMessagePublishHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked after a message has been published.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void AfterMessagePublished(Func<MessagePublishContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        AfterMessagePublishedHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked before a message is handled.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void BeforeMessageHandle(Func<MessageHandleContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        BeforeMessageHandleHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked after a message has been handled.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void AfterMessageHandled(Func<MessageHandleContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        AfterMessageHandledHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked before the message service stops.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void BeforeMessageServiceStop(Func<MessageServiceContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        BeforeMessageServiceStopHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked after the message service has stopped.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void AfterMessageServiceStopped(Func<MessageServiceContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        AfterMessageServiceStoppedHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked when a message handler throws an exception.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void OnMessageHandleError(Func<MessageErrorContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        OnMessageHandleErrorHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked when publishing a message fails.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void OnMessagePublishError(Func<MessageErrorContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        OnMessagePublishErrorHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked when deserializing a consumed message fails.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void OnMessageDeserializeError(Func<MessageErrorContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        OnMessageDeserializeErrorHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked when serializing a message for publish fails.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void OnMessageSerializeError(Func<MessageErrorContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        OnMessageSerializeErrorHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked before a failed message is requeued for retry.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void OnMessageRetry(Func<MessageErrorContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        OnMessageRetryHooks.AddLast(hook);
    }

    /// <summary>
    /// Registers a hook invoked when a message has reached its retry limit and will be dead-lettered.
    /// Hooks are appended to the end of the collection and invoked in registration order (FIFO).
    /// </summary>
    /// <param name="hook">The hook to invoke.</param>
    public void OnRetryLimitReached(Func<MessageErrorContext, Task> hook)
    {
        ArgumentNullException.ThrowIfNull(hook);
        OnRetryLimitReachedHooks.AddLast(hook);
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

        if (OutboxOptions != null)
        {
            OutboxOptions.Validate();
        }

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

    internal static async Task InvokeHooksAsync<TContext>(
        IEnumerable<Func<TContext, Task>> hooks,
        TContext context)
    {
        foreach (var hook in hooks)
        {
            await hook(context);
        }
    }

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
