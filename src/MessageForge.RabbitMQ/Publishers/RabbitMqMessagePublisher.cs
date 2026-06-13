using MessageForge.RabbitMQ.ConnectionPools;
using RabbitMQ.Client;

namespace MessageForge.RabbitMQ.Publishers;

internal static class RabbitMqMessagePublisher
{
    internal static async Task PublishRawAsync(
        IConnectionPool connectionPool,
        string messageType,
        byte[] payload,
        CancellationToken cancellationToken)
    {
        var connection = connectionPool.GetConnection();

        var channelOptions = new CreateChannelOptions(
            publisherConfirmationsEnabled: true,
            publisherConfirmationTrackingEnabled: true);

        using var channel = await connection.CreateChannelAsync(channelOptions, cancellationToken: cancellationToken);

        await channel.BasicPublishAsync(
            exchange: messageType,
            routingKey: string.Empty,
            mandatory: false,
            body: payload,
            basicProperties: new BasicProperties { Type = messageType, Persistent = true },
            cancellationToken: cancellationToken);
    }
}
