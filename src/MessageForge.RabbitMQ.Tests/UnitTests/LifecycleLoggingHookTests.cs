using System.Diagnostics;
using MessageForge.Persistence.Outbox.Lifecycle;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Lifecycle;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class LifecycleLoggingHookTests
{
    [TestCase(nameof(MessageServiceOptions.BeforeMessageServiceStartHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageServiceStartedHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessageServiceStopHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageServiceStoppedHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessagePublishHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterMessagePublishedHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessageHandleHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageHandledHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessageHandleErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessagePublishErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessageDeserializeErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessageSerializeErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessageRetryHooks))]
    [TestCase(nameof(MessageServiceOptions.OnRetryLimitReachedHooks))]
    public void Register_Adds_Logging_Hook_To_Each_Collection(string hooksPropertyName)
    {
        // arrange
        var options = new MessageServiceOptions();

        // act
        LifecycleLoggingHooks.Register(options);

        // assert
        GetHookCount(options, hooksPropertyName).ShouldBe(1);
    }

    [TestCase(nameof(MessageServiceOptions.BeforeOutboxEnqueueHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterOutboxEnqueuedHooks))]
    [TestCase(nameof(MessageServiceOptions.OnOutboxSerializeErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeOutboxDispatchHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterOutboxDispatchedHooks))]
    [TestCase(nameof(MessageServiceOptions.OnOutboxDispatchErrorHooks))]
    public void Register_Adds_Outbox_Logging_Hook_To_Each_Collection(string hooksPropertyName)
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });

        // act
        LifecycleLoggingHooks.Register(options);

        // assert
        GetOutboxHookCount(options, hooksPropertyName).ShouldBe(1);
    }

    [Test]
    public void AddMessageForgeRabbitMQ_Registers_Default_Logging_Hooks()
    {
        // arrange
        var services = new ServiceCollection();

        // act
        services.AddMessageForgeRabbitMQ(options =>
            options.UseConnectionString("amqp://localhost"));

        var options = services.BuildServiceProvider().GetRequiredService<MessageServiceOptions>();

        // assert
        options.BeforeMessagePublishHooks.Count.ShouldBe(2);
        options.BeforeMessageHandleHooks.Count.ShouldBe(2);
        options.OnMessagePublishErrorHooks.Count.ShouldBe(2);
    }

    [Test]
    public async Task Default_Logging_Hook_Runs_Before_Telemetry_Hook()
    {
        // arrange
        var options = new MessageServiceOptions();
        var events = new List<string>();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(new TestLoggerProvider(events)));
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

        LifecycleTelemetryHooks.Register(options);
        LifecycleLoggingHooks.Register(options);
        options.BeforeMessagePublish(_ =>
        {
            events.Add("user");
            return Task.CompletedTask;
        });

        var context = new MessagePublishContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage { String = "SECRET_BODY_CONTENT" },
            MessageType = typeof(TestSimpleMessage),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessagePublishHooks, context);

        // assert
        events.Count.ShouldBe(2);
        events[0].ShouldStartWith("log:");
        events[1].ShouldBe("user");
        context.Activity.ShouldNotBeNull();
        context.Activity!.Source.Name.ShouldBe(MessageForgeActivitySource.Name);
    }

    [Test]
    public async Task Default_Logging_Hook_Does_Not_Log_Message_Body()
    {
        // arrange
        var options = new MessageServiceOptions();
        var events = new List<string>();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(new TestLoggerProvider(events)));
        var serviceProvider = services.BuildServiceProvider();
        const string secretBody = "SECRET_BODY_CONTENT";

        LifecycleLoggingHooks.Register(options);

        var context = new MessagePublishContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage { String = secretBody },
            MessageType = typeof(TestSimpleMessage),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessagePublishHooks, context);

        // assert
        events.ShouldNotBeEmpty();
        events.ShouldAllBe(e => !e.Contains(secretBody, StringComparison.Ordinal));
    }

    [Test]
    public async Task Default_Outbox_Logging_Hooks_Do_Not_Log_Message_Body()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        var events = new List<string>();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(new TestLoggerProvider(events)));
        var serviceProvider = services.BuildServiceProvider();
        const string secretBody = "SECRET_BODY_CONTENT";
        var message = new TestSimpleMessage { String = secretBody };
        var payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(message);

        LifecycleLoggingHooks.Register(options);

        var enqueueContext = new OutboxEnqueueContext
        {
            ServiceProvider = serviceProvider,
            Message = message,
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            CancellationToken = CancellationToken.None,
        };

        var dispatchContext = new OutboxDispatchContext
        {
            ServiceProvider = serviceProvider,
            OutboxMessageId = Guid.NewGuid(),
            MessageType = typeof(TestSimpleMessage).FullName!,
            Payload = payload,
            CancellationToken = CancellationToken.None,
        };

        var errorContext = new OutboxErrorContext
        {
            ServiceProvider = serviceProvider,
            Message = message,
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            DispatchedMessageType = typeof(TestSimpleMessage).FullName,
            Payload = payload,
            Exception = new InvalidOperationException("dispatch failed"),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeOutboxEnqueueHooks, enqueueContext);
        await MessageServiceOptions.InvokeHooksAsync(options.AfterOutboxEnqueuedHooks, enqueueContext);
        await MessageServiceOptions.InvokeHooksAsync(options.OnOutboxSerializeErrorHooks, errorContext);
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeOutboxDispatchHooks, dispatchContext);
        await MessageServiceOptions.InvokeHooksAsync(options.AfterOutboxDispatchedHooks, dispatchContext);
        await MessageServiceOptions.InvokeHooksAsync(options.OnOutboxDispatchErrorHooks, errorContext);

        // assert
        events.ShouldNotBeEmpty();
        events.ShouldAllBe(e => !e.Contains(secretBody, StringComparison.Ordinal));
    }

    [Test]
    public async Task Default_Outbox_Logging_Hooks_Do_Not_Log_Message_Body_When_Telemetry_Content_Is_Enabled()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        options.IncludeMessageContentInOpenTelemetry();
        var events = new List<string>();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(new TestLoggerProvider(events)));
        var serviceProvider = services.BuildServiceProvider();
        const string secretBody = "SECRET_BODY_CONTENT";

        LifecycleLoggingHooks.Register(options);

        var context = new OutboxEnqueueContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage { String = secretBody },
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeOutboxEnqueueHooks, context);

        // assert
        events.ShouldNotBeEmpty();
        events.ShouldAllBe(e => !e.Contains(secretBody, StringComparison.Ordinal));
    }

    [Test]
    public async Task AfterMessageHandled_Logging_Hook_Logs_Warning_When_HandleAsync_Return_Type_Is_Unexpected()
    {
        // arrange
        var options = new MessageServiceOptions();
        var events = new List<string>();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(new TestLoggerProvider(events)));
        var serviceProvider = services.BuildServiceProvider();

        LifecycleLoggingHooks.Register(options);

        var context = new MessageHandleContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage(),
            MessageType = typeof(TestSimpleMessage),
            DeliveryCount = 0,
            HandleAsyncReturnedUnexpectedType = true,
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.AfterMessageHandledHooks, context);

        // assert
        events.ShouldContain(e => e.StartsWith("log:Warning:", StringComparison.Ordinal));
        events.ShouldContain(e => e.Contains("did not return Task or ValueTask", StringComparison.Ordinal));
    }

    private static int GetHookCount(MessageServiceOptions options, string hooksPropertyName) =>
        hooksPropertyName switch
        {
            nameof(MessageServiceOptions.BeforeMessageServiceStartHooks) => options.BeforeMessageServiceStartHooks.Count,
            nameof(MessageServiceOptions.AfterMessageServiceStartedHooks) => options.AfterMessageServiceStartedHooks.Count,
            nameof(MessageServiceOptions.BeforeMessageServiceStopHooks) => options.BeforeMessageServiceStopHooks.Count,
            nameof(MessageServiceOptions.AfterMessageServiceStoppedHooks) => options.AfterMessageServiceStoppedHooks.Count,
            nameof(MessageServiceOptions.BeforeMessagePublishHooks) => options.BeforeMessagePublishHooks.Count,
            nameof(MessageServiceOptions.AfterMessagePublishedHooks) => options.AfterMessagePublishedHooks.Count,
            nameof(MessageServiceOptions.BeforeMessageHandleHooks) => options.BeforeMessageHandleHooks.Count,
            nameof(MessageServiceOptions.AfterMessageHandledHooks) => options.AfterMessageHandledHooks.Count,
            nameof(MessageServiceOptions.OnMessageHandleErrorHooks) => options.OnMessageHandleErrorHooks.Count,
            nameof(MessageServiceOptions.OnMessagePublishErrorHooks) => options.OnMessagePublishErrorHooks.Count,
            nameof(MessageServiceOptions.OnMessageDeserializeErrorHooks) => options.OnMessageDeserializeErrorHooks.Count,
            nameof(MessageServiceOptions.OnMessageSerializeErrorHooks) => options.OnMessageSerializeErrorHooks.Count,
            nameof(MessageServiceOptions.OnMessageRetryHooks) => options.OnMessageRetryHooks.Count,
            nameof(MessageServiceOptions.OnRetryLimitReachedHooks) => options.OnRetryLimitReachedHooks.Count,
            _ => throw new ArgumentOutOfRangeException(nameof(hooksPropertyName)),
        };

    private static int GetOutboxHookCount(MessageServiceOptions options, string hooksPropertyName) =>
        hooksPropertyName switch
        {
            nameof(MessageServiceOptions.BeforeOutboxEnqueueHooks) => options.BeforeOutboxEnqueueHooks.Count,
            nameof(MessageServiceOptions.AfterOutboxEnqueuedHooks) => options.AfterOutboxEnqueuedHooks.Count,
            nameof(MessageServiceOptions.OnOutboxSerializeErrorHooks) => options.OnOutboxSerializeErrorHooks.Count,
            nameof(MessageServiceOptions.BeforeOutboxDispatchHooks) => options.BeforeOutboxDispatchHooks.Count,
            nameof(MessageServiceOptions.AfterOutboxDispatchedHooks) => options.AfterOutboxDispatchedHooks.Count,
            nameof(MessageServiceOptions.OnOutboxDispatchErrorHooks) => options.OnOutboxDispatchErrorHooks.Count,
            _ => throw new ArgumentOutOfRangeException(nameof(hooksPropertyName)),
        };

    private static ActivityListener CreateActivityListener()
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessageForgeActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };

        ActivitySource.AddActivityListener(listener);
        return listener;
    }

    private sealed class TestLoggerProvider(List<string> events) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new TestLogger(events);

        public void Dispose()
        {
        }
    }

    private sealed class TestLogger(List<string> events) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            events.Add($"log:{logLevel}:{formatter(state, exception)}");
        }
    }
}
