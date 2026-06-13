using System.Diagnostics;

namespace MessageForge.RabbitMQ.Lifecycle;

/// <summary>
/// Context passed to message error lifecycle hooks.
/// </summary>
public sealed class MessageErrorContext
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
    public required Type MessageType { get; init; }

    /// <summary>
    /// Gets the exception that caused the error, if any.
    /// </summary>
    public Exception? Exception { get; init; }

    /// <summary>
    /// Gets the number of times the message has been delivered.
    /// </summary>
    public int DeliveryCount { get; init; }

    /// <summary>
    /// Gets a value indicating whether the message will be retried.
    /// </summary>
    public bool WillRetry { get; init; }

    /// <summary>
    /// Gets a value indicating whether the message will be dead-lettered.
    /// </summary>
    public bool WillDeadLetter { get; init; }

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> for the current operation.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// Gets the in-flight lifecycle <see cref="Activity"/> for the current operation, if any.
    /// </summary>
    public Activity? Activity { get; init; }
}
