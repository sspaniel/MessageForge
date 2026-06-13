using System.Diagnostics;
using MessageForge.RabbitMQ.Services;

namespace MessageForge.RabbitMQ.Lifecycle;

internal static class LifecycleTelemetryHooks
{
    internal static void Register(MessageServiceOptions options)
    {
        options.BeforeMessageServiceStartHooks.AddFirst(ctx =>
        {
            ctx.Activity = MessageForgeActivitySource.Instance.StartActivity(
                "messageforge.service.start",
                ActivityKind.Server);
            return Task.CompletedTask;
        });

        options.AfterMessageServiceStartedHooks.AddFirst(ctx =>
        {
            CompleteActivity(ctx.Activity);
            ctx.Activity = null;
            return Task.CompletedTask;
        });

        options.BeforeMessageServiceStopHooks.AddFirst(ctx =>
        {
            ctx.Activity = MessageForgeActivitySource.Instance.StartActivity(
                "messageforge.service.stop",
                ActivityKind.Server);
            return Task.CompletedTask;
        });

        options.AfterMessageServiceStoppedHooks.AddFirst(ctx =>
        {
            CompleteActivity(ctx.Activity);
            ctx.Activity = null;
            return Task.CompletedTask;
        });

        options.BeforeMessagePublishHooks.AddFirst(ctx =>
        {
            var activity = MessageForgeActivitySource.Instance.StartActivity(
                "messageforge.message.publish",
                ActivityKind.Producer);
            SetMessageTypeTags(activity, ctx.MessageType);
            ctx.Activity = activity;
            return Task.CompletedTask;
        });

        options.AfterMessagePublishedHooks.AddFirst(ctx =>
        {
            CompleteActivity(ctx.Activity);
            ctx.Activity = null;
            return Task.CompletedTask;
        });

        options.BeforeMessageHandleHooks.AddFirst(ctx =>
        {
            var activity = MessageForgeActivitySource.Instance.StartActivity(
                "messageforge.message.handle",
                ActivityKind.Consumer);
            SetMessageTypeTags(activity, ctx.MessageType);
            activity?.SetTag("messaging.message.delivery.count", ctx.DeliveryCount);
            ctx.Activity = activity;
            return Task.CompletedTask;
        });

        options.AfterMessageHandledHooks.AddFirst(ctx =>
        {
            if (ctx.HandleAsyncReturnedUnexpectedType)
            {
                ctx.Activity?.SetTag("messaging.handle.unexpected_return", true);
            }

            CompleteActivity(ctx.Activity);
            ctx.Activity = null;
            return Task.CompletedTask;
        });

        options.OnMessageSerializeErrorHooks.AddFirst(ctx =>
        {
            RecordErrorActivity("messageforge.message.serialize_error", ctx.MessageType, ctx.Exception);
            return Task.CompletedTask;
        });

        options.OnMessagePublishErrorHooks.AddFirst(ctx =>
        {
            RecordErrorActivity("messageforge.message.publish_error", ctx.MessageType, ctx.Exception);
            return Task.CompletedTask;
        });

        options.OnMessageDeserializeErrorHooks.AddFirst(ctx =>
        {
            using var activity = MessageForgeActivitySource.Instance.StartActivity(
                "messageforge.message.deserialize_error",
                ActivityKind.Consumer);
            SetMessageTypeTags(activity, ctx.MessageType);
            activity?.SetTag("messaging.message.delivery.count", ctx.DeliveryCount);
            activity?.SetTag("messaging.dead_letter", ctx.WillDeadLetter);
            RecordException(activity, ctx.Exception);
            return Task.CompletedTask;
        });

        options.OnMessageHandleErrorHooks.AddFirst(ctx =>
        {
            using var activity = MessageForgeActivitySource.Instance.StartActivity(
                "messageforge.message.handle_error",
                ActivityKind.Consumer);
            SetMessageTypeTags(activity, ctx.MessageType);
            activity?.SetTag("messaging.message.delivery.count", ctx.DeliveryCount);
            RecordException(activity, ctx.Exception);
            return Task.CompletedTask;
        });

        options.OnMessageRetryHooks.AddFirst(ctx =>
        {
            using var activity = MessageForgeActivitySource.Instance.StartActivity(
                "messageforge.message.retry",
                ActivityKind.Consumer);
            SetMessageTypeTags(activity, ctx.MessageType);
            activity?.SetTag("messaging.message.delivery.count", ctx.DeliveryCount);
            activity?.SetStatus(ActivityStatusCode.Ok);
            return Task.CompletedTask;
        });

        options.OnRetryLimitReachedHooks.AddFirst(ctx =>
        {
            using var activity = MessageForgeActivitySource.Instance.StartActivity(
                "messageforge.message.retry_limit_reached",
                ActivityKind.Consumer);
            SetMessageTypeTags(activity, ctx.MessageType);
            activity?.SetTag("messaging.message.delivery.count", ctx.DeliveryCount);
            activity?.SetTag("messaging.dead_letter", true);
            activity?.SetStatus(ActivityStatusCode.Error, "Retry limit reached.");
            return Task.CompletedTask;
        });
    }

    private static void SetMessageTypeTags(Activity? activity, Type messageType)
    {
        activity?.SetTag("messaging.system", "rabbitmq");
        activity?.SetTag("messaging.message.type", messageType.FullName);
    }

    private static void RecordErrorActivity(string name, Type messageType, Exception? exception)
    {
        using var activity = MessageForgeActivitySource.Instance.StartActivity(name, ActivityKind.Internal);
        SetMessageTypeTags(activity, messageType);
        RecordException(activity, exception);
    }

    private static void RecordException(Activity? activity, Exception? exception)
    {
        if (activity is null)
        {
            return;
        }

        if (exception is not null)
        {
            activity.SetStatus(ActivityStatusCode.Error, exception.Message);
            activity.AddException(exception);
        }
        else
        {
            activity.SetStatus(ActivityStatusCode.Error);
        }
    }

    private static void CompleteActivity(Activity? activity)
    {
        if (activity is null)
        {
            return;
        }

        if (activity.Status == ActivityStatusCode.Unset)
        {
            activity.SetStatus(ActivityStatusCode.Ok);
        }

        activity.Dispose();
    }
}
