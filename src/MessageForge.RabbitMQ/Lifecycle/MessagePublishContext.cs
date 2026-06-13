namespace MessageForge.RabbitMQ.Lifecycle;

/// <summary>
/// Context passed to the message publish lifecycle hooks.
/// </summary>
public sealed class MessagePublishContext
{
    /// <summary>
    /// Gets the application <see cref="IServiceProvider"/>.
    /// </summary>
    required public IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the message being published.
    /// </summary>
    required public object Message { get; init; }

    /// <summary>
    /// Gets the type of the message being published.
    /// </summary>
    required public Type MessageType { get; init; }

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> for the current operation.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }
}
