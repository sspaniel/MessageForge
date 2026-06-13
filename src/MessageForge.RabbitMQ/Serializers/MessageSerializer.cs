using System.Text.Json;
using System.Text.Json.Serialization;
using RabbitMQ.Client.Events;

namespace MessageForge.RabbitMQ.Serializers;

internal sealed class MessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new JsonSerializerOptions()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    /// <inheritdoc />
    public object? Deserialize(Type messsageType, BasicDeliverEventArgs eventArgs)
    {
        var message = JsonSerializer.Deserialize(eventArgs.Body.Span, messsageType, Options);
        return message;
    }

    /// <inheritdoc />
    public byte[] Serialize<TMessage>(TMessage message)
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        return JsonSerializer.SerializeToUtf8Bytes(message, Options);
    }
}
