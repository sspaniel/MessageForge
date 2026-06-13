namespace MessageForge.Persistence.Outbox;

/// <summary>
/// A message stored in the transactional outbox pending dispatch to the message broker.
/// </summary>
public sealed class OutboxMessage
{
    /// <summary>
    /// Gets or sets the unique identifier for the outbox message.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Gets or sets the monotonic dequeue sequence assigned when the message is enqueued.
    /// </summary>
    public long Sequence { get; set; }

    /// <summary>
    /// Gets or sets the fully qualified CLR type name of the message.
    /// </summary>
    required public string MessageType { get; set; }

    /// <summary>
    /// Gets or sets the serialized message payload.
    /// </summary>
    required public byte[] Payload { get; set; }

    /// <summary>
    /// Gets or sets when the message was enqueued.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; }
}
