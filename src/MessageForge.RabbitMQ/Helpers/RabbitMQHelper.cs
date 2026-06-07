using MessageForge.Errors;
using MessageForge.RabbitMQ.Services;
using RabbitMQ.Client;

namespace MessageForge.RabbitMQ.Helpers;

internal static class RabbitMQHelper
{
    public static readonly Dictionary<string, object?> DefaultQueueArgs = new Dictionary<string, object?>
    {
        { "x-dead-letter-exchange", MessagingService.DeadLetterExchangeName },
        { "x-queue-type", "quorum" },
    };

    public static async Task CreateDefaultExchangesAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.ExchangeDeclareAsync(
            exchange: typeof(MessageForgeError).FullName ?? throw new ArgumentNullException(nameof(MessageForgeError)),
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);

        await channel.ExchangeDeclareAsync(
            exchange: MessagingService.DeadLetterExchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            cancellationToken: cancellationToken);
    }

    public static async Task CreateDefaultQueuesAsync(IChannel channel, CancellationToken cancellationToken)
    {
        await channel.QueueDeclareAsync(
            queue: MessagingService.DeadLetterQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: DefaultQueueArgs,
            cancellationToken: cancellationToken);

        await channel.QueueDeclareAsync(
            queue: MessagingService.ErrorQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: DefaultQueueArgs,
            cancellationToken: cancellationToken);

        await channel.QueueBindAsync(
            queue: MessagingService.DeadLetterQueueName,
            exchange: MessagingService.DeadLetterExchangeName,
            routingKey: string.Empty,
            cancellationToken: cancellationToken);

        await channel.QueueBindAsync(
            queue: MessagingService.ErrorQueueName,
            exchange: typeof(MessageForgeError).FullName ?? throw new ArgumentNullException(nameof(MessageForgeError)),
            routingKey: string.Empty,
            cancellationToken: cancellationToken);
    }
}
