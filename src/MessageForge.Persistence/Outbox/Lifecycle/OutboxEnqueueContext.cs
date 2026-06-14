using System.Diagnostics;

namespace MessageForge.Persistence.Outbox.Lifecycle;

/// <summary>
/// Context passed to outbox enqueue lifecycle hooks.
/// </summary>
public sealed class OutboxEnqueueContext
{
    /// <summary>
    /// Gets the application <see cref="IServiceProvider"/>.
    /// </summary>
    required public IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the message being enqueued.
    /// </summary>
    required public object Message { get; init; }

    /// <summary>
    /// Gets the type of the message being enqueued.
    /// </summary>
    required public Type MessageType { get; init; }

    /// <summary>
    /// Gets the outbox message identifier assigned for this enqueue operation.
    /// </summary>
    required public Guid OutboxMessageId { get; init; }

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> for the current operation.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// Gets or sets the current lifecycle <see cref="Activity"/>.
    /// </summary>
    public Activity? Activity { get; set; }
}
