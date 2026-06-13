using MessageForge.RabbitMQ.Publishers;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Subscribers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MessageForge.RabbitMQ.Lifecycle;

internal static class LifecycleLoggingHooks
{
    internal static void Register(MessageServiceOptions options)
    {
        options.BeforeMessageServiceStartHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<MessageService>>();
            logger.LogInformation("Starting message service.");
            return Task.CompletedTask;
        });

        options.AfterMessageServiceStartedHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<MessageService>>();
            logger.LogInformation("Message service started.");
            return Task.CompletedTask;
        });

        options.BeforeMessageServiceStopHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<MessageService>>();
            logger.LogInformation("Stopping message service.");
            return Task.CompletedTask;
        });

        options.AfterMessageServiceStoppedHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<MessageService>>();
            logger.LogInformation("Message service stopped.");
            return Task.CompletedTask;
        });

        options.BeforeMessagePublishHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<Publisher>>();
            logger.LogInformation("Publishing message of type {MessageType}.", ctx.MessageType.Name);
            return Task.CompletedTask;
        });

        options.AfterMessagePublishedHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<Publisher>>();
            logger.LogInformation("Published message of type {MessageType}.", ctx.MessageType.Name);
            return Task.CompletedTask;
        });

        options.BeforeMessageHandleHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<RabbitMQSubscriber>>();
            logger.LogInformation(
                "Handling message of type {MessageType} (delivery count {DeliveryCount}).",
                ctx.MessageType.Name,
                ctx.DeliveryCount);
            return Task.CompletedTask;
        });

        options.AfterMessageHandledHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<RabbitMQSubscriber>>();

            if (ctx.HandleAsyncReturnedUnexpectedType)
            {
                logger.LogWarning(
                    "HandleAsync on message type {MessageType} did not return Task or ValueTask.",
                    ctx.MessageType.Name);
            }

            logger.LogInformation(
                "Handled message of type {MessageType} (delivery count {DeliveryCount}).",
                ctx.MessageType.Name,
                ctx.DeliveryCount);
            return Task.CompletedTask;
        });

        options.OnMessageSerializeErrorHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<Publisher>>();
            logger.LogError(
                ctx.Exception,
                "Error serializing message of type {MessageType}.",
                ctx.MessageType.Name);
            return Task.CompletedTask;
        });

        options.OnMessagePublishErrorHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<Publisher>>();
            logger.LogError(
                ctx.Exception,
                "Error publishing message of type {MessageType}.",
                ctx.MessageType.Name);
            return Task.CompletedTask;
        });

        options.OnMessageDeserializeErrorHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<RabbitMQSubscriber>>();
            logger.LogError(
                ctx.Exception,
                "Failed to deserialize message of type {MessageType} (delivery count {DeliveryCount}, will dead-letter {WillDeadLetter}).",
                ctx.MessageType.Name,
                ctx.DeliveryCount,
                ctx.WillDeadLetter);
            return Task.CompletedTask;
        });

        options.OnMessageHandleErrorHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<RabbitMQSubscriber>>();
            logger.LogError(
                ctx.Exception,
                "Failed to process message of type {MessageType} (delivery count {DeliveryCount}).",
                ctx.MessageType.Name,
                ctx.DeliveryCount);
            return Task.CompletedTask;
        });

        options.OnMessageRetryHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<RabbitMQSubscriber>>();
            logger.LogWarning(
                "Retrying message of type {MessageType} (delivery count {DeliveryCount}).",
                ctx.MessageType.Name,
                ctx.DeliveryCount);
            return Task.CompletedTask;
        });

        options.OnRetryLimitReachedHooks.AddFirst(ctx =>
        {
            var logger = ctx.ServiceProvider.GetRequiredService<ILogger<RabbitMQSubscriber>>();
            logger.LogWarning(
                "Retry limit reached for message of type {MessageType} (delivery count {DeliveryCount}).",
                ctx.MessageType.Name,
                ctx.DeliveryCount);
            return Task.CompletedTask;
        });
    }
}
