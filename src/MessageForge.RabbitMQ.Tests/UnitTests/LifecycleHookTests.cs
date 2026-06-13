using System.Reflection;
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
    [TestCase(nameof(MessageServiceOptions.BeforeSubscriberInitializeHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterSubscriberInitializedHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeSubscriberStartHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterSubscriberStartedHooks))]
    [TestCase(nameof(MessageServiceOptions.BeforeSubscriberStopHooks))]
    [TestCase(nameof(MessageServiceOptions.AfterSubscriberStoppedHooks))]
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

    [Test]
    public async Task BeforeSubscriberInitialize_Context_Includes_Subscriber_Details()
    {
        // arrange
        var options = new MessageServiceOptions();
        MessageSubscriberContext? capturedContext = null;
        var serviceProvider = new ServiceCollection().BuildServiceProvider();

        options.BeforeSubscriberInitialize(context =>
        {
            capturedContext = context;
            return Task.CompletedTask;
        });

        var context = new MessageSubscriberContext
        {
            ServiceProvider = serviceProvider,
            SubscriberType = typeof(TestSubscriber),
            MessageType = typeof(TestSimpleMessage),
            QueueName = "test-queue",
            CancellationToken = CancellationToken.None,
        };

        // act
        await MessageServiceOptions.InvokeHooksAsync(options.BeforeSubscriberInitializeHooks, context);

        // assert
        capturedContext.ShouldNotBeNull();
        capturedContext.SubscriberType.ShouldBe(typeof(TestSubscriber));
        capturedContext.MessageType.ShouldBe(typeof(TestSimpleMessage));
        capturedContext.QueueName.ShouldBe("test-queue");
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
    [TestCase(nameof(MessageServiceOptions.BeforeSubscriberInitialize))]
    [TestCase(nameof(MessageServiceOptions.AfterSubscriberInitialized))]
    [TestCase(nameof(MessageServiceOptions.BeforeSubscriberStart))]
    [TestCase(nameof(MessageServiceOptions.AfterSubscriberStarted))]
    [TestCase(nameof(MessageServiceOptions.BeforeSubscriberStop))]
    [TestCase(nameof(MessageServiceOptions.AfterSubscriberStopped))]
    public void Lifecycle_Hook_Registration_Throws_When_Hook_Is_Null(string methodName)
    {
        // arrange
        var options = new MessageServiceOptions();
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
                case nameof(MessageServiceOptions.BeforeSubscriberInitializeHooks):
                case nameof(MessageServiceOptions.AfterSubscriberInitializedHooks):
                case nameof(MessageServiceOptions.BeforeSubscriberStartHooks):
                case nameof(MessageServiceOptions.AfterSubscriberStartedHooks):
                case nameof(MessageServiceOptions.BeforeSubscriberStopHooks):
                case nameof(MessageServiceOptions.AfterSubscriberStoppedHooks):
                    RegisterSubscriberHook(options, hooksPropertyName, invocationOrder, hookId);
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

    private static void RegisterSubscriberHook(
        MessageServiceOptions options,
        string hooksPropertyName,
        List<int> invocationOrder,
        int hookId)
    {
        Func<MessageSubscriberContext, Task> hook = _ =>
        {
            invocationOrder.Add(hookId);
            return Task.CompletedTask;
        };

        switch (hooksPropertyName)
        {
            case nameof(MessageServiceOptions.BeforeSubscriberInitializeHooks):
                options.BeforeSubscriberInitialize(hook);
                break;
            case nameof(MessageServiceOptions.AfterSubscriberInitializedHooks):
                options.AfterSubscriberInitialized(hook);
                break;
            case nameof(MessageServiceOptions.BeforeSubscriberStartHooks):
                options.BeforeSubscriberStart(hook);
                break;
            case nameof(MessageServiceOptions.AfterSubscriberStartedHooks):
                options.AfterSubscriberStarted(hook);
                break;
            case nameof(MessageServiceOptions.BeforeSubscriberStopHooks):
                options.BeforeSubscriberStop(hook);
                break;
            case nameof(MessageServiceOptions.AfterSubscriberStoppedHooks):
                options.AfterSubscriberStopped(hook);
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
            case nameof(MessageServiceOptions.BeforeSubscriberInitializeHooks):
            case nameof(MessageServiceOptions.AfterSubscriberInitializedHooks):
            case nameof(MessageServiceOptions.BeforeSubscriberStartHooks):
            case nameof(MessageServiceOptions.AfterSubscriberStartedHooks):
            case nameof(MessageServiceOptions.BeforeSubscriberStopHooks):
            case nameof(MessageServiceOptions.AfterSubscriberStoppedHooks):
                await InvokeSubscriberHooksAsync(options, hooksPropertyName, serviceProvider);
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

    private static async Task InvokeSubscriberHooksAsync(
        MessageServiceOptions options,
        string hooksPropertyName,
        IServiceProvider serviceProvider)
    {
        var context = new MessageSubscriberContext
        {
            ServiceProvider = serviceProvider,
            SubscriberType = typeof(TestSubscriber),
            MessageType = typeof(TestSimpleMessage),
            QueueName = "test-queue",
            CancellationToken = CancellationToken.None,
        };

        var hooks = hooksPropertyName switch
        {
            nameof(MessageServiceOptions.BeforeSubscriberInitializeHooks) => options.BeforeSubscriberInitializeHooks,
            nameof(MessageServiceOptions.AfterSubscriberInitializedHooks) => options.AfterSubscriberInitializedHooks,
            nameof(MessageServiceOptions.BeforeSubscriberStartHooks) => options.BeforeSubscriberStartHooks,
            nameof(MessageServiceOptions.AfterSubscriberStartedHooks) => options.AfterSubscriberStartedHooks,
            nameof(MessageServiceOptions.BeforeSubscriberStopHooks) => options.BeforeSubscriberStopHooks,
            nameof(MessageServiceOptions.AfterSubscriberStoppedHooks) => options.AfterSubscriberStoppedHooks,
            _ => throw new ArgumentOutOfRangeException(nameof(hooksPropertyName)),
        };

        await MessageServiceOptions.InvokeHooksAsync(hooks, context);
    }

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
            nameof(MessageServiceOptions.BeforeSubscriberInitializeHooks) => options.BeforeSubscriberInitializeHooks.Count,
            nameof(MessageServiceOptions.AfterSubscriberInitializedHooks) => options.AfterSubscriberInitializedHooks.Count,
            nameof(MessageServiceOptions.BeforeSubscriberStartHooks) => options.BeforeSubscriberStartHooks.Count,
            nameof(MessageServiceOptions.AfterSubscriberStartedHooks) => options.AfterSubscriberStartedHooks.Count,
            nameof(MessageServiceOptions.BeforeSubscriberStopHooks) => options.BeforeSubscriberStopHooks.Count,
            nameof(MessageServiceOptions.AfterSubscriberStoppedHooks) => options.AfterSubscriberStoppedHooks.Count,
            _ => throw new ArgumentOutOfRangeException(nameof(hooksPropertyName)),
        };
}
