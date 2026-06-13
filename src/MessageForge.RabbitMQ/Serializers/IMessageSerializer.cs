using RabbitMQ.Client.Events;

namespace MessageForge.RabbitMQ.Serializers;

/// <summary>
/// Message serializer interface.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Deserializes the specified event args to a message of type TMessage. Returns null if deserialization fails.
    /// </summary>
    /// <param name="messsageType">The type of the message to deserialize.</param>
    /// <param name="eventArgs">The event args to deserialize.</param>
    object? Deserialize(Type messsageType, BasicDeliverEventArgs eventArgs);

    /// <summary>
    /// Serializes the specified value to a byte array of utf8 characters.
    /// </summary>
    /// <typeparam name="TMessage">Type of message to serialize.</typeparam>
    /// <param name="message">The message to serialize.</param>
    byte[] Serialize<TMessage>(TMessage message);
}
