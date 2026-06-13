namespace MessageForge.Persistence.Outbox;

/// <summary>
/// Serializes outbox message payloads.
/// </summary>
internal interface IOutboxMessageSerializer
{
    /// <summary>
    /// Serializes the specified message to a UTF-8 JSON byte array.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="message">The message to serialize.</param>
    byte[] Serialize<TMessage>(TMessage message);
}
