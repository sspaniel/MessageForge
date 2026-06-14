using System.Diagnostics;
using MessageForge.Persistence.Outbox.Lifecycle;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Lifecycle;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Trace;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class LifecycleTelemetryHookTests
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
    public void Register_Adds_Telemetry_Hook_To_Each_Collection(string hooksPropertyName)
    {
        // arrange
        var options = new MessageServiceOptions();

        // act
        LifecycleTelemetryHooks.Register(options);

        // assert
        GetHookCount(options, hooksPropertyName).ShouldBe(1);
    }

    [TestCase(nameof(MessageServiceOptions.BeforeOutboxEnqueueHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterOutboxEnqueuedHooks))]
    [TestCase(nameof(MessageServiceOptions.OnOutboxSerializeErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeOutboxDispatchHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterOutboxDispatchedHooks))]
    [TestCase(nameof(MessageServiceOptions.OnOutboxDispatchErrorHooks))]
    public void Register_Adds_Outbox_Telemetry_Hook_To_Each_Collection(string hooksPropertyName)
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });

        // act
        LifecycleTelemetryHooks.Register(options);

        // assert
        GetOutboxHookCount(options, hooksPropertyName).ShouldBe(1);
    }

    [Test]
    public void AddMessageForgeRabbitMQ_Registers_Default_Telemetry_And_Logging_Hooks()
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
    public async Task AddMessageForgeRabbitMQ_Starts_TracerProvider_For_MessageForge_Activity_Source()
    {
        // arrange
        using var host = Host.CreateDefaultBuilder()
            .ConfigureServices(services =>
            {
                services.AddMessageForgeRabbitMQ(options =>
                    options.UseConnectionString("amqp://localhost"));
            })
            .Build();

        // act
        await host.StartAsync();

        try
        {
            // assert
            host.Services.GetService<TracerProvider>().ShouldNotBeNull();
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Test]
    public async Task Telemetry_Hooks_Run_After_Logging_Hooks_And_Before_User_Hooks()
    {
        // arrange
        var options = new MessageServiceOptions();
        var events = new List<string>();

        LifecycleTelemetryHooks.Register(options);
        LifecycleLoggingHooks.Register(options);
        options.BeforeMessagePublish(_ =>
        {
            events.Add("user");
            return Task.CompletedTask;
        });

        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(new TestLoggerProvider(events)));
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

        var context = new MessagePublishContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage { String = "SECRET_BODY_CONTENT" },
            MessageType = typeof(TestSimpleMessage),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessagePublishHooks, context);

        // assert — logging first, telemetry sets activity, user hook last
        events.Count.ShouldBe(2);
        events[0].ShouldStartWith("log:");
        events[1].ShouldBe("user");
        context.Activity.ShouldNotBeNull();
        context.Activity!.Source.Name.ShouldBe(MessageForgeActivitySource.Name);
        context.Activity.OperationName.ShouldBe("messageforge.message.publish");
        context.Activity.GetTagItem("messaging.message.type").ShouldBe(typeof(TestSimpleMessage).FullName);
        context.Activity.GetTagItem("messaging.message.type").ShouldNotBe("SECRET_BODY_CONTENT");
        events.ShouldAllBe(e => !e.Contains("SECRET_BODY_CONTENT", StringComparison.Ordinal));
        context.Activity!.GetTagItem("messaging.message.body").ShouldBeNull();
    }

    [Test]
    public async Task BeforeMessagePublish_Attaches_To_Existing_Activity_As_Child()
    {
        // arrange
        var options = new MessageServiceOptions();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        using var parentSource = new ActivitySource("Test.Parent");

        using var listener = new ActivityListener
        {
            ShouldListenTo = _ => true,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };

        ActivitySource.AddActivityListener(listener);

        using var parentActivity = parentSource.StartActivity("parent.operation");
        parentActivity.ShouldNotBeNull();

        var context = new MessagePublishContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage(),
            MessageType = typeof(TestSimpleMessage),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessagePublishHooks, context);

        // assert
        context.Activity.ShouldNotBeNull();
        context.Activity!.ParentId.ShouldBe(parentActivity!.Id);
    }

    [Test]
    public async Task Publish_Error_Hook_Attaches_To_In_Flight_Publish_Activity_As_Child()
    {
        // arrange
        var options = new MessageServiceOptions();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        Activity? recordedActivity = null;

        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessageForgeActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => recordedActivity = activity,
        };

        ActivitySource.AddActivityListener(listener);

        var publishContext = new MessagePublishContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage(),
            MessageType = typeof(TestSimpleMessage),
            CancellationToken = CancellationToken.None,
        };

        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessagePublishHooks, publishContext);
        var publishActivity = publishContext.Activity;
        publishActivity.ShouldNotBeNull();

        var errorContext = new MessageErrorContext
        {
            ServiceProvider = serviceProvider,
            MessageType = typeof(TestSimpleMessage),
            Exception = new InvalidOperationException("publish failed"),
            CancellationToken = CancellationToken.None,
            Activity = publishActivity,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.OnMessagePublishErrorHooks, errorContext);

        // assert
        recordedActivity.ShouldNotBeNull();
        recordedActivity!.ParentId.ShouldBe(publishActivity!.Id);
    }

    [Test]
    public async Task BeforeMessagePublish_Includes_Message_Body_When_Enabled()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.IncludeMessageContentInOpenTelemetry();

        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

        const string secretBody = "SECRET_BODY_CONTENT";
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
        context.Activity.ShouldNotBeNull();
        context.Activity!.GetTagItem("messaging.message.body")
            .ShouldBe("{\"Guid\":\"00000000-0000-0000-0000-000000000000\",\"String\":\"SECRET_BODY_CONTENT\",\"Integer\":0,\"Float\":0,\"DateTime\":\"0001-01-01T00:00:00\"}");
    }

    [Test]
    public async Task BeforeMessagePublish_Excludes_Message_Body_By_Default()
    {
        // arrange
        var options = new MessageServiceOptions();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

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
        context.Activity.ShouldNotBeNull();
        context.Activity!.GetTagItem("messaging.message.body").ShouldBeNull();
    }

    [Test]
    public async Task Before_And_After_Publish_Hooks_Complete_Activity()
    {
        // arrange
        var options = new MessageServiceOptions();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

        var context = new MessagePublishContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage(),
            MessageType = typeof(TestSimpleMessage),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessagePublishHooks, context);
        var startedActivity = context.Activity;
        await MessageServiceOptions.InvokeHooksAsync(options.AfterMessagePublishedHooks, context);

        // assert
        startedActivity.ShouldNotBeNull();
        startedActivity!.Status.ShouldBe(ActivityStatusCode.Ok);
        context.Activity.ShouldBeNull();
    }

    [Test]
    public async Task Publish_Error_Hook_Records_Exception_On_Activity()
    {
        // arrange
        var options = new MessageServiceOptions();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        Activity? recordedActivity = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessageForgeActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => recordedActivity = activity,
        };
        ActivitySource.AddActivityListener(listener);

        var exception = new InvalidOperationException("publish failed");
        var context = new MessageErrorContext
        {
            ServiceProvider = serviceProvider,
            MessageType = typeof(TestSimpleMessage),
            Exception = exception,
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.OnMessagePublishErrorHooks, context);

        // assert
        recordedActivity.ShouldNotBeNull();
        recordedActivity!.OperationName.ShouldBe("messageforge.message.publish_error");
        recordedActivity.Status.ShouldBe(ActivityStatusCode.Error);
        recordedActivity.Tags.Any(t => t.Key == "messaging.message.type").ShouldBeTrue();
    }

    [Test]
    public async Task BeforeOutboxEnqueue_Excludes_Message_Body_By_Default()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

        var context = new OutboxEnqueueContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage { String = "SECRET_BODY_CONTENT" },
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeOutboxEnqueueHooks, context);

        // assert
        context.Activity.ShouldNotBeNull();
        context.Activity!.GetTagItem("messaging.message.body").ShouldBeNull();
    }

    [Test]
    public async Task BeforeOutboxEnqueue_Includes_Message_Body_When_Enabled()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        options.IncludeMessageContentInOpenTelemetry();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

        const string secretBody = "SECRET_BODY_CONTENT";
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
        context.Activity.ShouldNotBeNull();
        context.Activity!.GetTagItem("messaging.message.body")
            .ShouldBe("{\"Guid\":\"00000000-0000-0000-0000-000000000000\",\"String\":\"SECRET_BODY_CONTENT\",\"Integer\":0,\"Float\":0,\"DateTime\":\"0001-01-01T00:00:00\"}");
    }

    [Test]
    public async Task BeforeOutboxDispatch_Excludes_Payload_By_Default()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

        var payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new TestSimpleMessage { String = "SECRET_BODY_CONTENT" });

        var context = new OutboxDispatchContext
        {
            ServiceProvider = serviceProvider,
            OutboxMessageId = Guid.NewGuid(),
            MessageType = typeof(TestSimpleMessage).FullName!,
            Payload = payload,
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeOutboxDispatchHooks, context);

        // assert
        context.Activity.ShouldNotBeNull();
        context.Activity!.GetTagItem("messaging.message.body").ShouldBeNull();
    }

    [Test]
    public async Task BeforeOutboxDispatch_Includes_Payload_When_Enabled()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        options.IncludeMessageContentInOpenTelemetry();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        using var listener = CreateActivityListener();

        const string secretBody = "SECRET_BODY_CONTENT";
        var payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new TestSimpleMessage { String = secretBody });

        var context = new OutboxDispatchContext
        {
            ServiceProvider = serviceProvider,
            OutboxMessageId = Guid.NewGuid(),
            MessageType = typeof(TestSimpleMessage).FullName!,
            Payload = payload,
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeOutboxDispatchHooks, context);

        // assert
        context.Activity.ShouldNotBeNull();
        context.Activity!.GetTagItem("messaging.message.body")
            .ShouldBe("{\"Guid\":\"00000000-0000-0000-0000-000000000000\",\"String\":\"SECRET_BODY_CONTENT\",\"Integer\":0,\"Float\":0,\"DateTime\":\"0001-01-01T00:00:00\"}");
    }

    [Test]
    public async Task OnOutboxSerializeError_Excludes_Message_Body_By_Default()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        Activity? recordedActivity = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessageForgeActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => recordedActivity = activity,
        };
        ActivitySource.AddActivityListener(listener);

        var context = new OutboxErrorContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage { String = "SECRET_BODY_CONTENT" },
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            Exception = new System.Text.Json.JsonException("serialize failed"),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.OnOutboxSerializeErrorHooks, context);

        // assert
        recordedActivity.ShouldNotBeNull();
        recordedActivity!.GetTagItem("messaging.message.body").ShouldBeNull();
    }

    [Test]
    public async Task OnOutboxSerializeError_Includes_Message_Body_When_Enabled()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        options.IncludeMessageContentInOpenTelemetry();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        Activity? recordedActivity = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessageForgeActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => recordedActivity = activity,
        };
        ActivitySource.AddActivityListener(listener);

        const string secretBody = "SECRET_BODY_CONTENT";
        var context = new OutboxErrorContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage { String = secretBody },
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            Exception = new System.Text.Json.JsonException("serialize failed"),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.OnOutboxSerializeErrorHooks, context);

        // assert
        recordedActivity.ShouldNotBeNull();
        recordedActivity!.GetTagItem("messaging.message.body")
            .ShouldBe("{\"Guid\":\"00000000-0000-0000-0000-000000000000\",\"String\":\"SECRET_BODY_CONTENT\",\"Integer\":0,\"Float\":0,\"DateTime\":\"0001-01-01T00:00:00\"}");
    }

    [Test]
    public async Task OnOutboxDispatchError_Excludes_Payload_By_Default()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        Activity? recordedActivity = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessageForgeActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => recordedActivity = activity,
        };
        ActivitySource.AddActivityListener(listener);

        var payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new TestSimpleMessage { String = "SECRET_BODY_CONTENT" });

        var context = new OutboxErrorContext
        {
            ServiceProvider = serviceProvider,
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            DispatchedMessageType = typeof(TestSimpleMessage).FullName,
            Payload = payload,
            Exception = new InvalidOperationException("dispatch failed"),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.OnOutboxDispatchErrorHooks, context);

        // assert
        recordedActivity.ShouldNotBeNull();
        recordedActivity!.GetTagItem("messaging.message.body").ShouldBeNull();
    }

    [Test]
    public async Task OnOutboxDispatchError_Includes_Payload_When_Enabled()
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        options.IncludeMessageContentInOpenTelemetry();
        LifecycleTelemetryHooks.Register(options);

        var services = new ServiceCollection();
        services.AddLogging();
        var serviceProvider = services.BuildServiceProvider();

        Activity? recordedActivity = null;
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == MessageForgeActivitySource.Name,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => recordedActivity = activity,
        };
        ActivitySource.AddActivityListener(listener);

        const string secretBody = "SECRET_BODY_CONTENT";
        var payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new TestSimpleMessage { String = secretBody });

        var context = new OutboxErrorContext
        {
            ServiceProvider = serviceProvider,
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            DispatchedMessageType = typeof(TestSimpleMessage).FullName,
            Payload = payload,
            Exception = new InvalidOperationException("dispatch failed"),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.OnOutboxDispatchErrorHooks, context);

        // assert
        recordedActivity.ShouldNotBeNull();
        recordedActivity!.GetTagItem("messaging.message.body")
            .ShouldBe("{\"Guid\":\"00000000-0000-0000-0000-000000000000\",\"String\":\"SECRET_BODY_CONTENT\",\"Integer\":0,\"Float\":0,\"DateTime\":\"0001-01-01T00:00:00\"}");
    }

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
