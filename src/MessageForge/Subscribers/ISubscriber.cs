namespace MessageForge.Subscribers;

/// <summary>
/// Interface for receiving messages from publishers.
/// </summary>
/// <typeparam name="TMessage">Type of message to receive.</typeparam>
public interface ISubscriber<TMessage>
    where TMessage : new()
{
    /// <summary>
    /// Receives a message from a publisher.
    /// </summary>
    /// <param name="message">Message received.</param>
    /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
    Task HandleAsync(TMessage message, CancellationToken cancellationToken = default);
}
