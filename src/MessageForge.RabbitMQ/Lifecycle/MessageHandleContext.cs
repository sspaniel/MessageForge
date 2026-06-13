namespace MessageForge.RabbitMQ.Lifecycle;

/// <summary>
/// Context passed to the message handle lifecycle hooks.
/// </summary>
public sealed class MessageHandleContext
{
    /// <summary>
    /// Gets the application <see cref="IServiceProvider"/>.
    /// </summary>
    required public IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the message being handled.
    /// </summary>
    required public object Message { get; init; }

    /// <summary>
    /// Gets the type of the message being handled.
    /// </summary>
    public required Type MessageType { get; init; }

    /// <summary>
    /// Gets the number of times the message has been delivered.
    /// </summary>
    public int DeliveryCount { get; init; }

    /// <summary>
    /// Gets a value indicating whether <c>HandleAsync</c> did not return <see cref="Task"/> or <see cref="ValueTask"/>.
    /// </summary>
    public bool HandleAsyncReturnedUnexpectedType { get; init; }

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> for the current operation.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }
}
