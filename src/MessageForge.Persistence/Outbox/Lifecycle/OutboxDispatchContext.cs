using System.Diagnostics;

namespace MessageForge.Persistence.Outbox.Lifecycle;

/// <summary>
/// Context passed to outbox dispatch lifecycle hooks.
/// </summary>
public sealed class OutboxDispatchContext
{
    /// <summary>
    /// Gets the application <see cref="IServiceProvider"/>.
    /// </summary>
    required public IServiceProvider ServiceProvider { get; init; }

    /// <summary>
    /// Gets the outbox message identifier being dispatched.
    /// </summary>
    required public Guid OutboxMessageId { get; init; }

    /// <summary>
    /// Gets the fully qualified CLR type name of the message.
    /// </summary>
    required public string MessageType { get; init; }

    /// <summary>
    /// Gets the serialized message payload.
    /// </summary>
    required public byte[] Payload { get; init; }

    /// <summary>
    /// Gets the <see cref="CancellationToken"/> for the current operation.
    /// </summary>
    public CancellationToken CancellationToken { get; init; }

    /// <summary>
    /// Gets or sets the current lifecycle <see cref="Activity"/>.
    /// </summary>
    public Activity? Activity { get; set; }
}
