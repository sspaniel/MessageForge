using System.Diagnostics;

namespace MessageForge.Persistence.Outbox.Lifecycle;

/// <summary>
/// Context passed to outbox error lifecycle hooks.
/// </summary>
public sealed class OutboxErrorContext
{
    /// <summary>
    /// Gets the application <see cref="IServiceProvider"/>.
    /// </summary>
    required public IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the message associated with the error, if available.
    /// </summary>
    public object? Message { get; init; }

    /// <summary>
    /// Gets the type of the message associated with the error.
    /// </summary>
    required public Type MessageType { get; init; }

    /// <summary>
    /// Gets the outbox message identifier associated with the error, if available.
    /// </summary>
    public Guid? OutboxMessageId { get; init; }

    /// <summary>
    /// Gets the fully qualified CLR type name of the message being dispatched, if applicable.
    /// </summary>
    public string? DispatchedMessageType { get; init; }

    /// <summary>
    /// Gets the serialized message payload associated with the error, if available.
    /// </summary>
    public byte[]? Payload { get; init; }

    /// <summary>
    /// Gets the exception that caused the error, if any.
    /// </summary>
    public Exception? Exception { get; init; }

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> for the current operation.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// Gets the in-flight lifecycle <see cref="Activity"/> for the current operation, if any.
    /// </summary>
    public Activity? Activity { get; init; }
}
