namespace MessageForge.RabbitMQ.Lifecycle;

/// <summary>
/// Context passed to the message service start lifecycle hooks.
/// </summary>
public sealed class MessageServiceContext
{
    /// <summary>
    /// Gets the application <see cref="IServiceProvider"/>.
    /// </summary>
    required public IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> for the current operation.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }
}
