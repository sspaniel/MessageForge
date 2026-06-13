namespace MessageForge.Persistence.Outbox;

/// <summary>
/// Dispatches serialized outbox messages to the message broker.
/// </summary>
internal interface IOutboxDispatcher
{
    /// <summary>
    /// Dispatches a serialized message to the broker.
    /// </summary>
    /// <param name="messageType">The fully qualified CLR type name of the message.</param>
    /// <param name="payload">The serialized message payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task DispatchAsync(string messageType, byte[] payload, CancellationToken cancellationToken = default);
}
