namespace MessageForge.RabbitMQ.Lifecycle;

/// <summary>
/// Context passed to subscriber infrastructure lifecycle hooks.
/// </summary>
public sealed class MessageSubscriberContext
{
    /// <summary>
    /// Gets the application <see cref="IServiceProvider"/>.
    /// </summary>
    required public IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the type of the subscriber.
    /// </summary>
    public required Type SubscriberType { get; init; }

    /// <summary>
    /// Gets the type of the message the subscriber handles.
    /// </summary>
    public required Type MessageType { get; init; }

    /// <summary>
    /// Gets the name of the subscriber queue.
    /// </summary>
    public required string QueueName { get; init; }

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> for the current operation.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }
}
