using System.Text.Json;
using System.Text.Json.Serialization;

namespace MessageForge.Persistence.Outbox;

internal sealed class JsonOutboxMessageSerializer : IOutboxMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new JsonSerializerOptions()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false,
    };

    /// <inheritdoc />
    public byte[] Serialize<TMessage>(TMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        return JsonSerializer.SerializeToUtf8Bytes(message, Options);
    }
}
