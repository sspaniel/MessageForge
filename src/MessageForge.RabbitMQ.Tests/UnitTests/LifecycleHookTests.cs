using System.Reflection;
using MessageForge.Persistence.Outbox.Lifecycle;
using MessageForge.RabbitMQ.Lifecycle;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class LifecycleHookTests
{
    [Test]
    public void BeforeMessagePublish_Allows_Multiple_Hooks()
    {
        // arrange
        var options = new MessageServiceOptions();
        var firstCalled = false;
        var secondCalled = false;

        // act
        options.BeforeMessagePublish(_ =>
        {
            firstCalled = true;
            return Task.CompletedTask;
        });
        options.BeforeMessagePublish(_ =>
        {
            secondCalled = true;
            return Task.CompletedTask;
        });

        // assert
        options.BeforeMessagePublishHooks.Count.ShouldBe(2);
        firstCalled.ShouldBeFalse();
        secondCalled.ShouldBeFalse();
    }

    [Test]
    public void BeforeMessagePublish_Throws_When_Hook_Is_Null()
    {
        // arrange
        var options = new MessageServiceOptions();

        // act / assert
        Should.Throw<ArgumentNullException>(() => options.BeforeMessagePublish(null!));
    }

    [Test]
    public async Task BeforeMessagePublish_Hooks_Are_Invoked_In_Registration_Order()
    {
        // arrange
        var options = new MessageServiceOptions();
        var invocationOrder = new List<int>();

        options.BeforeMessagePublish(_ =>
        {
            invocationOrder.Add(1);
            return Task.CompletedTask;
        });
        options.BeforeMessagePublish(_ =>
        {
            invocationOrder.Add(2);
            return Task.CompletedTask;
        });

        var message = new TestSimpleMessage();
        var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var context = new MessagePublishContext
        {
            ServiceProvider = serviceProvider,
            Message = message,
            MessageType = typeof(TestSimpleMessage),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessagePublishHooks, context);

        // assert
        invocationOrder.ShouldBe([1, 2]);
    }

    [TestCase(nameof(MessageServiceOptions.BeforeMessageServiceStartHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageServiceStartedHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessagePublishHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterMessagePublishedHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessageHandleHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageHandledHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessageServiceStopHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageServiceStoppedHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessageHandleErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessagePublishErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessageDeserializeErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessageSerializeErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnMessageRetryHooks))]
    [TestCase(nameof(MessageServiceOptions.OnRetryLimitReachedHooks))]
    public async Task Lifecycle_Hooks_Are_Appended_And_Invoked_In_Fifo_Order(string hooksPropertyName)
    {
        // arrange
        var options = new MessageServiceOptions();
        var invocationOrder = new List<int>();

        RegisterHooks(options, hooksPropertyName, invocationOrder, 1, 2, 3);

        // act
        await InvokeHooksAsync(options, hooksPropertyName);

        // assert
        GetHookCount(options, hooksPropertyName).ShouldBe(3);
        invocationOrder.ShouldBe([1, 2, 3]);
    }

    [TestCase(nameof(MessageServiceOptions.BeforeOutboxEnqueueHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterOutboxEnqueuedHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeOutboxDispatchHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterOutboxDispatchedHooks))]
    public async Task Outbox_Lifecycle_Hooks_Are_Appended_And_Invoked_In_Fifo_Order(string hooksPropertyName)
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        var invocationOrder = new List<int>();

        RegisterOutboxHooks(options, hooksPropertyName, invocationOrder, 1, 2, 3);

        // act
        await InvokeOutboxHooksAsync(options, hooksPropertyName);

        // assert
        GetOutboxHookCount(options, hooksPropertyName).ShouldBe(3);
        invocationOrder.ShouldBe([1, 2, 3]);
    }

    [TestCase(nameof(MessageServiceOptions.OnOutboxSerializeErrorHooks))]
    [TestCase(nameof(MessageServiceOptions.OnOutboxDispatchErrorHooks))]
    public async Task Outbox_Error_Hooks_Are_Appended_And_Invoked_In_Fifo_Order(string hooksPropertyName)
    {
        // arrange
        var options = new MessageServiceOptions();
        options.ConfigureOutbox(_ => { });
        var invocationOrder = new List<int>();

        RegisterOutboxErrorHooks(options, hooksPropertyName, invocationOrder, 1, 2, 3);

        // act
        await InvokeOutboxErrorHooksAsync(options, hooksPropertyName);

        // assert
        GetOutboxHookCount(options, hooksPropertyName).ShouldBe(3);
        invocationOrder.ShouldBe([1, 2, 3]);
    }

    [Test]
    public async Task Lifecycle_Hooks_Are_Invoked_In_Fifo_Order()
    {
        // arrange
        var options = new MessageServiceOptions();
        var invocationOrder = new List<int>();

        options.BeforeMessagePublish(_ =>
        {
            invocationOrder.Add(1);
            return Task.CompletedTask;
        });
        options.BeforeMessagePublish(_ =>
        {
            invocationOrder.Add(2);
            return Task.CompletedTask;
        });
        options.BeforeMessagePublish(_ =>
        {
            invocationOrder.Add(3);
            return Task.CompletedTask;
        });

        var context = new MessagePublishContext
        {
            ServiceProvider = new ServiceCollection().BuildServiceProvider(),
            Message = new TestSimpleMessage(),
            MessageType = typeof(TestSimpleMessage),
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessagePublishHooks, context);

        // assert
        invocationOrder.ShouldBe([1, 2, 3]);
    }

    [Test]
    public async Task BeforeMessagePublish_Context_Includes_ServiceProvider()
    {
        // arrange
        var options = new MessageServiceOptions();
        IServiceProvider? capturedServiceProvider = null;
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        options.BeforeMessagePublish(context =>
        {
            capturedServiceProvider = context.ServiceProvider;
            return Task.CompletedTask;
        });

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
        capturedServiceProvider.ShouldBeSameAs(serviceProvider);
    }

    [Test]
    public async Task BeforeMessageHandle_Context_Includes_ServiceProvider()
    {
        // arrange
        var options = new MessageServiceOptions();
        IServiceProvider? capturedServiceProvider = null;
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        options.BeforeMessageHandle(context =>
        {
            capturedServiceProvider = context.ServiceProvider;
            return Task.CompletedTask;
        });

        var context = new MessageHandleContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage(),
            MessageType = typeof(TestSimpleMessage),
            DeliveryCount = 0,
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessageHandleHooks, context);

        // assert
        capturedServiceProvider.ShouldBeSameAs(serviceProvider);
    }

    [Test]
    public async Task BeforeMessageServiceStart_Context_Includes_ServiceProvider()
    {
        // arrange
        var options = new MessageServiceOptions();
        IServiceProvider? capturedServiceProvider = null;
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        options.BeforeMessageServiceStart(context =>
        {
            capturedServiceProvider = context.ServiceProvider;
            return Task.CompletedTask;
        });

        var context = new MessageServiceContext
        {
            ServiceProvider = serviceProvider,
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeMessageServiceStartHooks, context);

        // assert
        capturedServiceProvider.ShouldBeSameAs(serviceProvider);
    }

    [Test]
    public async Task OnMessageHandleError_Context_Includes_Error_Details()
    {
        // arrange
        var options = new MessageServiceOptions();
        MessageErrorContext? capturedContext = null;
        var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var exception = new InvalidOperationException("handle failed");
        var message = new TestSimpleMessage();

        options.OnMessageHandleError(context =>
        {
            capturedContext = context;
            return Task.CompletedTask;
        });

        var context = new MessageErrorContext
        {
            ServiceProvider = serviceProvider,
            Message = message,
            MessageType = typeof(TestSimpleMessage),
            Exception = exception,
            DeliveryCount = 2,
            WillRetry = true,
            WillDeadLetter = false,
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.OnMessageHandleErrorHooks, context);

        // assert
        capturedContext.ShouldNotBeNull();
        capturedContext.ServiceProvider.ShouldBeSameAs(serviceProvider);
        capturedContext.Message.ShouldBeSameAs(message);
        capturedContext.Exception.ShouldBeSameAs(exception);
        capturedContext.DeliveryCount.ShouldBe(2);
        capturedContext.WillRetry.ShouldBeTrue();
        capturedContext.WillDeadLetter.ShouldBeFalse();
    }

    [TestCase(nameof(MessageServiceOptions.BeforeMessageServiceStart))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageServiceStarted))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessagePublish))]
    [TestCase(nameof(MessageServiceOptions.AfterMessagePublished))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessageHandle))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageHandled))]
    [TestCase(nameof(MessageServiceOptions.BeforeMessageServiceStop))]
    [TestCase(nameof(MessageServiceOptions.AfterMessageServiceStopped))]
    [TestCase(nameof(MessageServiceOptions.OnMessageHandleError))]
    [TestCase(nameof(MessageServiceOptions.OnMessagePublishError))]
    [TestCase(nameof(MessageServiceOptions.OnMessageDeserializeError))]
    [TestCase(nameof(MessageServiceOptions.OnMessageSerializeError))]
    [TestCase(nameof(MessageServiceOptions.OnMessageRetry))]
    [TestCase(nameof(MessageServiceOptions.OnRetryLimitReached))]
    [TestCase(nameof(MessageServiceOptions.BeforeOutboxEnqueue))]
    [TestCase(nameof(MessageServiceOptions.AfterOutboxEnqueued))]
    [TestCase(nameof(MessageServiceOptions.OnOutboxSerializeError))]
    [TestCase(nameof(MessageServiceOptions.BeforeOutboxDispatch))]
    [TestCase(nameof(MessageServiceOptions.AfterOutboxDispatched))]
    [TestCase(nameof(MessageServiceOptions.OnOutboxDispatchError))]
    public void Lifecycle_Hook_Registration_Throws_When_Hook_Is_Null(string methodName)
    {
        // arrange
        var options = new MessageServiceOptions();
        if (methodName.StartsWith("BeforeOutbox", StringComparison.Ordinal)
            || methodName.StartsWith("AfterOutbox", StringComparison.Ordinal)
            || methodName.StartsWith("OnOutbox", StringComparison.Ordinal))
        {
            options.ConfigureOutbox(_ => { });
        }

        var method = typeof(MessageServiceOptions).GetMethod(methodName)!;

        // act / assert
        Should.Throw<TargetInvocationException>(() =>
            method.Invoke(options, [null!]))
            .InnerException.ShouldBeOfType<ArgumentNullException>();
    }

    private static void RegisterHooks(
        MessageServiceOptions options,
        string hooksPropertyName,
        List<int> invocationOrder,
        params int[] hookIds)
    {
        foreach (var hookId in hookIds)
        {
            switch (hooksPropertyName)
            {
                case nameof(MessageServiceOptions.BeforeMessageServiceStartHooks):
                    options.BeforeMessageServiceStart(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.AfterMessageServiceStartedHooks):
                    options.AfterMessageServiceStarted(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.BeforeMessagePublishHooks):
                    options.BeforeMessagePublish(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.AfterMessagePublishedHooks):
                    options.AfterMessagePublished(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.BeforeMessageHandleHooks):
                    options.BeforeMessageHandle(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.AfterMessageHandledHooks):
                    options.AfterMessageHandled(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.BeforeMessageServiceStopHooks):
                    options.BeforeMessageServiceStop(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.AfterMessageServiceStoppedHooks):
                    options.AfterMessageServiceStopped(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.OnMessageHandleErrorHooks):
                case nameof(MessageServiceOptions.OnMessagePublishErrorHooks):
                case nameof(MessageServiceOptions.OnMessageDeserializeErrorHooks):
                case nameof(MessageServiceOptions.OnMessageSerializeErrorHooks):
                case nameof(MessageServiceOptions.OnMessageRetryHooks):
                case nameof(MessageServiceOptions.OnRetryLimitReachedHooks):
                    RegisterErrorHook(options, hooksPropertyName, invocationOrder, hookId);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(hooksPropertyName));
            }
        }
    }

    private static void RegisterErrorHook(
        MessageServiceOptions options,
        string hooksPropertyName,
        List<int> invocationOrder,
        int hookId)
    {
        Func<MessageErrorContext, Task> hook = _ =>
        {
            invocationOrder.Add(hookId);
            return Task.CompletedTask;
        };

        switch (hooksPropertyName)
        {
            case nameof(MessageServiceOptions.OnMessageHandleErrorHooks):
                options.OnMessageHandleError(hook);
                break;
            case nameof(MessageServiceOptions.OnMessagePublishErrorHooks):
                options.OnMessagePublishError(hook);
                break;
            case nameof(MessageServiceOptions.OnMessageDeserializeErrorHooks):
                options.OnMessageDeserializeError(hook);
                break;
            case nameof(MessageServiceOptions.OnMessageSerializeErrorHooks):
                options.OnMessageSerializeError(hook);
                break;
            case nameof(MessageServiceOptions.OnMessageRetryHooks):
                options.OnMessageRetry(hook);
                break;
            case nameof(MessageServiceOptions.OnRetryLimitReachedHooks):
                options.OnRetryLimitReached(hook);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(hooksPropertyName));
        }
    }

    private static async Task InvokeHooksAsync(MessageServiceOptions options, string hooksPropertyName)
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        switch (hooksPropertyName)
        {
            case nameof(MessageServiceOptions.BeforeMessageServiceStartHooks):
                await MessageServiceOptions.InvokeHooksAsync(
                    options.BeforeMessageServiceStartHooks,
                    new MessageServiceContext
                    {
                        ServiceProvider = serviceProvider,
                        CancellationToken = CancellationToken.None,
                    });
                break;
            case nameof(MessageServiceOptions.AfterMessageServiceStartedHooks):
                await MessageServiceOptions.InvokeHooksAsync(
                    options.AfterMessageServiceStartedHooks,
                    new MessageServiceContext
                    {
                        ServiceProvider = serviceProvider,
                        CancellationToken = CancellationToken.None,
                    });
                break;
            case nameof(MessageServiceOptions.BeforeMessagePublishHooks):
            case nameof(MessageServiceOptions.AfterMessagePublishedHooks):
                await MessageServiceOptions.InvokeHooksAsync(
                    hooksPropertyName == nameof(MessageServiceOptions.BeforeMessagePublishHooks)
                        ? options.BeforeMessagePublishHooks
                        : options.AfterMessagePublishedHooks,
                    new MessagePublishContext
                    {
                        ServiceProvider = serviceProvider,
                        Message = new TestSimpleMessage(),
                        MessageType = typeof(TestSimpleMessage),
                        CancellationToken = CancellationToken.None,
                    });
                break;
            case nameof(MessageServiceOptions.BeforeMessageHandleHooks):
            case nameof(MessageServiceOptions.AfterMessageHandledHooks):
                await MessageServiceOptions.InvokeHooksAsync(
                    hooksPropertyName == nameof(MessageServiceOptions.BeforeMessageHandleHooks)
                        ? options.BeforeMessageHandleHooks
                        : options.AfterMessageHandledHooks,
                    new MessageHandleContext
                    {
                        ServiceProvider = serviceProvider,
                        Message = new TestSimpleMessage(),
                        MessageType = typeof(TestSimpleMessage),
                        DeliveryCount = 0,
                        CancellationToken = CancellationToken.None,
                    });
                break;
            case nameof(MessageServiceOptions.BeforeMessageServiceStopHooks):
            case nameof(MessageServiceOptions.AfterMessageServiceStoppedHooks):
                await MessageServiceOptions.InvokeHooksAsync(
                    hooksPropertyName == nameof(MessageServiceOptions.BeforeMessageServiceStopHooks)
                        ? options.BeforeMessageServiceStopHooks
                        : options.AfterMessageServiceStoppedHooks,
                    new MessageServiceContext
                    {
                        ServiceProvider = serviceProvider,
                        CancellationToken = CancellationToken.None,
                    });
                break;
            case nameof(MessageServiceOptions.OnMessageHandleErrorHooks):
            case nameof(MessageServiceOptions.OnMessagePublishErrorHooks):
            case nameof(MessageServiceOptions.OnMessageDeserializeErrorHooks):
            case nameof(MessageServiceOptions.OnMessageSerializeErrorHooks):
            case nameof(MessageServiceOptions.OnMessageRetryHooks):
            case nameof(MessageServiceOptions.OnRetryLimitReachedHooks):
                await InvokeErrorHooksAsync(options, hooksPropertyName, serviceProvider);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(hooksPropertyName));
        }
    }

    private static async Task InvokeErrorHooksAsync(
        MessageServiceOptions options,
        string hooksPropertyName,
        IServiceProvider serviceProvider)
    {
        var context = new MessageErrorContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage(),
            MessageType = typeof(TestSimpleMessage),
            Exception = new InvalidOperationException("test"),
            DeliveryCount = 1,
            WillRetry = true,
            WillDeadLetter = false,
            CancellationToken = CancellationToken.None,
        };

        var hooks = hooksPropertyName switch
        {
            nameof(MessageServiceOptions.OnMessageHandleErrorHooks) => options.OnMessageHandleErrorHooks,
            nameof(MessageServiceOptions.OnMessagePublishErrorHooks) => options.OnMessagePublishErrorHooks,
            nameof(MessageServiceOptions.OnMessageDeserializeErrorHooks) => options.OnMessageDeserializeErrorHooks,
            nameof(MessageServiceOptions.OnMessageSerializeErrorHooks) => options.OnMessageSerializeErrorHooks,
            nameof(MessageServiceOptions.OnMessageRetryHooks) => options.OnMessageRetryHooks,
            nameof(MessageServiceOptions.OnRetryLimitReachedHooks) => options.OnRetryLimitReachedHooks,
            _ => throw new ArgumentOutOfRangeException(nameof(hooksPropertyName)),
        };

        await MessageServiceOptions.InvokeHooksAsync(hooks, context);
    }

    private static void RegisterOutboxHooks(
        MessageServiceOptions options,
        string hooksPropertyName,
        List<int> invocationOrder,
        params int[] hookIds)
    {
        foreach (var hookId in hookIds)
        {
            switch (hooksPropertyName)
            {
                case nameof(MessageServiceOptions.BeforeOutboxEnqueueHooks):
                    options.BeforeOutboxEnqueue(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.AfterOutboxEnqueuedHooks):
                    options.AfterOutboxEnqueued(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.BeforeOutboxDispatchHooks):
                    options.BeforeOutboxDispatch(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                case nameof(MessageServiceOptions.AfterOutboxDispatchedHooks):
                    options.AfterOutboxDispatched(_ =>
                    {
                        invocationOrder.Add(hookId);
                        return Task.CompletedTask;
                    });
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(hooksPropertyName));
            }
        }
    }

    private static void RegisterOutboxErrorHooks(
        MessageServiceOptions options,
        string hooksPropertyName,
        List<int> invocationOrder,
        params int[] hookIds)
    {
        foreach (var hookId in hookIds)
        {
            Func<OutboxErrorContext, Task> hook = _ =>
            {
                invocationOrder.Add(hookId);
                return Task.CompletedTask;
            };

            switch (hooksPropertyName)
            {
                case nameof(MessageServiceOptions.OnOutboxSerializeErrorHooks):
                    options.OnOutboxSerializeError(hook);
                    break;
                case nameof(MessageServiceOptions.OnOutboxDispatchErrorHooks):
                    options.OnOutboxDispatchError(hook);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(hooksPropertyName));
            }
        }
    }

    private static async Task InvokeOutboxHooksAsync(MessageServiceOptions options, string hooksPropertyName)
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        switch (hooksPropertyName)
        {
            case nameof(MessageServiceOptions.BeforeOutboxEnqueueHooks):
            case nameof(MessageServiceOptions.AfterOutboxEnqueuedHooks):
                await MessageServiceOptions.InvokeHooksAsync(
                    hooksPropertyName == nameof(MessageServiceOptions.BeforeOutboxEnqueueHooks)
                        ? options.BeforeOutboxEnqueueHooks
                        : options.AfterOutboxEnqueuedHooks,
                    new OutboxEnqueueContext
                    {
                        ServiceProvider = serviceProvider,
                        Message = new TestSimpleMessage(),
                        MessageType = typeof(TestSimpleMessage),
                        OutboxMessageId = Guid.NewGuid(),
                        CancellationToken = CancellationToken.None,
                    });
                break;
            case nameof(MessageServiceOptions.BeforeOutboxDispatchHooks):
            case nameof(MessageServiceOptions.AfterOutboxDispatchedHooks):
                await MessageServiceOptions.InvokeHooksAsync(
                    hooksPropertyName == nameof(MessageServiceOptions.BeforeOutboxDispatchHooks)
                        ? options.BeforeOutboxDispatchHooks
                        : options.AfterOutboxDispatchedHooks,
                    new OutboxDispatchContext
                    {
                        ServiceProvider = serviceProvider,
                        OutboxMessageId = Guid.NewGuid(),
                        MessageType = typeof(TestSimpleMessage).FullName!,
                        Payload = [],
                        CancellationToken = CancellationToken.None,
                    });
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(hooksPropertyName));
        }
    }

    private static async Task InvokeOutboxErrorHooksAsync(MessageServiceOptions options, string hooksPropertyName)
    {
        var serviceProvider = new ServiceCollection().BuildServiceProvider();
        var context = new OutboxErrorContext
        {
            ServiceProvider = serviceProvider,
            Message = new TestSimpleMessage(),
            MessageType = typeof(TestSimpleMessage),
            OutboxMessageId = Guid.NewGuid(),
            DispatchedMessageType = typeof(TestSimpleMessage).FullName,
            Exception = new InvalidOperationException("test"),
            CancellationToken = CancellationToken.None,
        };

        var hooks = hooksPropertyName switch
        {
            nameof(MessageServiceOptions.OnOutboxSerializeErrorHooks) => options.OnOutboxSerializeErrorHooks,
            nameof(MessageServiceOptions.OnOutboxDispatchErrorHooks) => options.OnOutboxDispatchErrorHooks,
            _ => throw new ArgumentOutOfRangeException(nameof(hooksPropertyName)),
        };

        await MessageServiceOptions.InvokeHooksAsync(hooks, context);
    }

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

    private static int GetHookCount(MessageServiceOptions options, string hooksPropertyName) =>
        hooksPropertyName switch
        {
            nameof(MessageServiceOptions.BeforeMessageServiceStartHooks) => options.BeforeMessageServiceStartHooks.Count,
            nameof(MessageServiceOptions.AfterMessageServiceStartedHooks) => options.AfterMessageServiceStartedHooks.Count,
            nameof(MessageServiceOptions.BeforeMessagePublishHooks) => options.BeforeMessagePublishHooks.Count,
            nameof(MessageServiceOptions.AfterMessagePublishedHooks) => options.AfterMessagePublishedHooks.Count,
            nameof(MessageServiceOptions.BeforeMessageHandleHooks) => options.BeforeMessageHandleHooks.Count,
            nameof(MessageServiceOptions.AfterMessageHandledHooks) => options.AfterMessageHandledHooks.Count,
            nameof(MessageServiceOptions.BeforeMessageServiceStopHooks) => options.BeforeMessageServiceStopHooks.Count,
            nameof(MessageServiceOptions.AfterMessageServiceStoppedHooks) => options.AfterMessageServiceStoppedHooks.Count,
            nameof(MessageServiceOptions.OnMessageHandleErrorHooks) => options.OnMessageHandleErrorHooks.Count,
            nameof(MessageServiceOptions.OnMessagePublishErrorHooks) => options.OnMessagePublishErrorHooks.Count,
            nameof(MessageServiceOptions.OnMessageDeserializeErrorHooks) => options.OnMessageDeserializeErrorHooks.Count,
            nameof(MessageServiceOptions.OnMessageSerializeErrorHooks) => options.OnMessageSerializeErrorHooks.Count,
            nameof(MessageServiceOptions.OnMessageRetryHooks) => options.OnMessageRetryHooks.Count,
            nameof(MessageServiceOptions.OnRetryLimitReachedHooks) => options.OnRetryLimitReachedHooks.Count,
            _ => throw new ArgumentOutOfRangeException(nameof(hooksPropertyName)),
        };
}
