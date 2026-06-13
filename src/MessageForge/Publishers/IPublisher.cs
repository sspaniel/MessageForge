namespace MessageForge.Publishers;

/// <summary>
/// Interface for sending messages to subscribers.
/// </summary>
public interface IPublisher
{
    /// <summary>
    /// Publishes a message to all subscribers.
    /// </summary>
    /// <typeparam name="TMessage">Type of message to send.</typeparam>
    /// <param name="message">Message to send.</param>
    /// <param name="cancellationToken"><see cref="CancellationToken"/>.</param>
    Task PublishAsync<TMessage>(TMessage message, CancellationToken cancellationToken = default)
        where TMessage : new();
}
