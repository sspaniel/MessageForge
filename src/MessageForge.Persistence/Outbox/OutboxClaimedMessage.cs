namespace MessageForge.Persistence.Outbox;

/// <summary>
/// A claimed outbox message containing only the fields required for dispatch.
/// </summary>
internal sealed class OutboxClaimedMessage
{
    /// <summary>
    /// Gets or sets the unique identifier for the outbox message.
    /// </summary>
    public Guid Id { get; set; }

    /// <summary>
    /// Gets or sets the fully qualified CLR type name of the message.
    /// </summary>
    required public string MessageType { get; set; }

    /// <summary>
    /// Gets or sets the serialized message payload.
    /// </summary>
    required public byte[] Payload { get; set; }
}
