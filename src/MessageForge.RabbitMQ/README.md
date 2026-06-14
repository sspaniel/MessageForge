# MessageForge.RabbitMQ

RabbitMQ integration for MessageForge. Publish and subscribe to messages using a fanout exchange per message type, with connection pooling, automatic queue provisioning, retries, dead-lettering, and OpenTelemetry tracing.

## Requirements

- .NET 10+
- A running [RabbitMQ](https://www.rabbitmq.com/) broker (quorum queues are used by default)

## Installation

```bash
dotnet add package MessageForge.RabbitMQ
```

This package references `MessageForge` and `MessageForge.Persistence`, and registers services for `Microsoft.Extensions.DependencyInjection` and `Microsoft.Extensions.Hosting`.

## Getting Started

### 1. Define message types

Message types must have a parameterless constructor (`new()` constraint). They are serialized as JSON and routed using the type's fully qualified name.

```csharp
public sealed class OrderPlaced
{
    public Guid OrderId { get; set; }
    public decimal Total { get; set; }
}
```

### 2. Implement subscribers

Create one or more classes that implement `ISubscriber<TMessage>`. A single class may implement multiple `ISubscriber<>` interfaces to handle different message types.

```csharp
using MessageForge.Subscribers;

public sealed class OrderPlacedHandler : ISubscriber<OrderPlaced>
{
    public Task HandleAsync(OrderPlaced message, CancellationToken cancellationToken = default)
    {
        // process the message
        return Task.CompletedTask;
    }
}
```

### 3. Register with dependency injection

Call `AddMessageForgeRabbitMQ` during service registration. When at least one subscriber is configured, a hosted `MessageService` is registered and starts consumers automatically with `IHost`.

```csharp
using MessageForge.RabbitMQ.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddMessageForgeRabbitMQ(options =>
{
    options.UseConnectionString("amqp://guest:guest@localhost:5672/");

    options.Subscribe<OrderPlacedHandler>(subscriber =>
    {
        subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromSeconds(1));
    });
});

using var host = builder.Build();
await host.RunAsync();
```

### 4. Publish messages

Inject `IPublisher` anywhere in your application and call `PublishAsync`. The message is published to a durable fanout exchange named after the message type's fully qualified name.

```csharp
using MessageForge.Publishers;

public sealed class OrderService(IPublisher publisher)
{
    public Task PlaceOrderAsync(Guid orderId, decimal total, CancellationToken cancellationToken = default) =>
        publisher.PublishAsync(
            new OrderPlaced { OrderId = orderId, Total = total },
            cancellationToken);
}
```

### Publisher-only applications

If you only publish messages and do not register any subscribers, `MessageService` is not added as a hosted service. Register subscribers on the consuming application instead.

## Persistence (transactional outbox)

Use the transactional outbox when messages must be published atomically with database changes. Instead of sending directly to RabbitMQ, `IPublisher` writes messages to an outbox table in the same database transaction as your application data. A background service then dispatches pending messages to the broker and removes them from the table after a successful publish.

This requires [Entity Framework Core](https://learn.microsoft.com/ef/core/) and the `MessageForge.Persistence` package (referenced automatically by `MessageForge.RabbitMQ`).

### 1. Define a database context

Inherit from `MessageForgeOutboxDbContext` and register the context with your EF Core provider as usual.

```csharp
using MessageForge.Persistence.Outbox;
using Microsoft.EntityFrameworkCore;

public sealed class AppDbContext : MessageForgeOutboxDbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options)
        : base(options)
    {
    }

    // your DbSets
}
```

Apply EF Core migrations (or `EnsureCreated` in development) so the `MessageForge.OutboxMessages` table exists. The outbox table stores pending messages with a monotonic `Sequence` column used for dequeue ordering.

### 2. Enable the outbox

Call `UseOutbox<TDbContext>()` inside `AddMessageForgeRabbitMQ`. When the outbox is enabled, `IPublisher` is registered as a scoped outbox writer and an `OutboxService` hosted service dispatches messages to RabbitMQ.

```csharp
using MessageForge.RabbitMQ.DependencyInjection;
using Microsoft.EntityFrameworkCore;

builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(builder.Configuration.GetConnectionString("Database")));

builder.Services.AddMessageForgeRabbitMQ(options =>
{
    options.UseConnectionString("amqp://guest:guest@localhost:5672/");

    options.UseOutbox<AppDbContext>(outbox =>
    {
        outbox.WithPollingInterval(TimeSpan.FromSeconds(1));
        outbox.WithBatchSize(100);
        outbox.WithDeduplication();
    });

    options.Subscribe<OrderPlacedHandler>();
});
```

### 3. Publish inside a unit of work

Inject `IUnitOfWork` and `IPublisher`. Begin a transaction, publish one or more messages, commit your application changes, then commit the unit of work. If the transaction is rolled back, outbox rows are not persisted and messages are never dispatched.

```csharp
using MessageForge.Persistence.UnitOfWork;
using MessageForge.Publishers;

public sealed class OrderService(IUnitOfWork unitOfWork, IPublisher publisher, AppDbContext db)
{
    public async Task PlaceOrderAsync(OrderPlaced order, CancellationToken cancellationToken = default)
    {
        db.Orders.Add(/* ... */);

        await unitOfWork.BeginAsync(cancellationToken);
        await publisher.PublishAsync(order, cancellationToken);
        await unitOfWork.CommitAsync(cancellationToken);
    }
}
```

`IUnitOfWork` and `IPublisher` are scoped services and must be resolved from the same DI scope as your `DbContext`.

### Outbox options

Configure outbox behavior through the delegate passed to `UseOutbox<TDbContext>()`.

| Method | Default | Description |
| --- | --- | --- |
| `WithPollingInterval(TimeSpan)` | `1` second | How often the service polls when the outbox is idle. When a full batch is pending, the service processes continuously until the backlog is drained. |
| `WithBatchSize(int)` | `100` | Maximum number of messages dispatched per service cycle. Increase this when draining large backlogs. |
| `WithDeduplication(bool)` | `true` | When enabled, skips publishing if a pending outbox row with the same `Id` already exists. |

### Deduplication

When deduplication is enabled, the outbox message `Id` is resolved from a `Guid Id` property on the message type when present and non-empty. Otherwise a new `Guid` is generated for each publish. Duplicate publishes with the same `Id` while a row is still pending are silently skipped.

```csharp
public sealed class OrderPlaced
{
    public Guid Id { get; set; }  // used as the outbox message Id for deduplication
    public decimal Total { get; set; }
}
```

After a message is successfully dispatched, the outbox row is deleted. The same `Id` can be published again in a later transaction.

### How the outbox service works

- Pending messages are read in `Sequence` order and dispatched in batches.
- After a successful broker publish, the row is removed from the outbox table.
- If dispatch fails (for example, the broker is unavailable), the row is retained and retried on the next polling cycle.
- Outbox lifecycle hooks (`BeforeOutboxEnqueue`, `AfterOutboxEnqueued`, `BeforeOutboxDispatch`, and related hooks) are invoked during enqueue and dispatch. Built-in logging and OpenTelemetry hooks are registered automatically when the outbox is enabled.

## How it works

- **Exchanges** — Each message type gets a durable fanout exchange named with the type's fully qualified name.
- **Queues** — Each subscriber registration creates a dedicated quorum queue named `{SubscriberType}:{MessageType}` and binds it to that message type's exchange. Multiple subscribers for the same message type each receive a copy (fanout).
- **Serialization** — Messages are serialized and deserialized with `System.Text.Json`.
- **Infrastructure queues** — On startup, the library provisions `MessageForge.DeadLetter` and `MessageForge.Errors` queues for dead-lettered messages and `MessageForgeError` notifications.
- **Subscribers** — Resolved from a new DI scope per message delivery.

## Configuration

All configuration is done through the `MessageServiceOptions` delegate passed to `AddMessageForgeRabbitMQ`.

### Connection

| Method | Default | Description |
| --- | --- | --- |
| `UseConnectionString(string)` | *(required)* | RabbitMQ connection URI (for example `amqp://user:pass@host:5672/vhost`). |
| `UseConnectionPoolSize(int)` | `Environment.ProcessorCount` | Maximum number of connections in the pool. Must be at least 1. |

```csharp
options.UseConnectionString("amqp://guest:guest@localhost:5672/");
options.UseConnectionPoolSize(8);
```

### Publisher

Configure publisher behavior with `ConfigureMessagePublisher`.

| Method | Default | Description |
| --- | --- | --- |
| `OnSerializationException(PublisherSerializerExceptionBehavior)` | `Ignore` | Controls behavior when JSON serialization fails before publish. `Throw` rethrows the exception; `Ignore` suppresses it and skips publishing. |

```csharp
options.ConfigureMessagePublisher(publisher =>
{
    publisher.OnSerializationException(PublisherSerializerExceptionBehavior.Throw);
});
```

### Subscribers

Register subscribers individually or scan an assembly for all concrete `ISubscriber<>` implementations.

| Method | Description |
| --- | --- |
| `Subscribe<TSubscriber>(Action<SubscriberOptions>)` | Registers a single subscriber type. If it implements multiple `ISubscriber<>` interfaces, each message type gets its own queue with the same options. |
| `AddSubscribersFromAssembly(Assembly, Action<SubscriberOptions>)` | Discovers and registers every concrete `ISubscriber<>` in the assembly, applying the same options to each. |

```csharp
// Single subscriber
options.Subscribe<OrderPlacedHandler>(subscriber => { /* ... */ });

// Assembly scan
options.AddSubscribersFromAssembly(typeof(OrderPlacedHandler).Assembly, subscriber => { /* ... */ });
```

#### Subscriber options

| Method | Default | Description |
| --- | --- | --- |
| `MessageTtl(TimeSpan)` | `TimeSpan.Zero` (no TTL) | Maximum time a message may remain in the queue before expiration. |
| `MaxMessageCount(int)` | `0` (unlimited) | Maximum number of messages allowed in the queue. |
| `MaxMessageConcurrency(ushort)` | `10` | Maximum number of messages processed concurrently by this subscriber. |
| `Retries(int maxRetryCount, TimeSpan retryDelay)` | `3` retries, `1` second delay | Number of delivery attempts before dead-lettering, and the delay between retries. Set `maxRetryCount` to `0` to dead-letter on the first failure. |
| `OnSerializationException(SubscriberSerializerExceptionBehavior)` | `Ignore` | `Ignore` removes the message from the queue; `DeadLetter` moves it to the dead-letter queue and publishes a `MessageForgeError` to the error queue. |

```csharp
options.Subscribe<OrderPlacedHandler>(subscriber =>
{
    subscriber.MessageTtl(TimeSpan.FromHours(1));
    subscriber.MaxMessageCount(10_000);
    subscriber.MaxMessageConcurrency(20);
    subscriber.Retries(maxRetryCount: 5, retryDelay: TimeSpan.FromSeconds(2));
    subscriber.OnSerializationException(SubscriberSerializerExceptionBehavior.DeadLetter);
});
```

When a handler throws, the message is negatively acknowledged and requeued until the retry limit is reached, then dead-lettered. Subscribe to `ISubscriber<MessageForgeError>` to receive structured error notifications from other consumers.

### OpenTelemetry

| Method | Default | Description |
| --- | --- | --- |
| `IncludeMessageContentInOpenTelemetry(bool)` | `false` | When enabled, serialized message content is added to OpenTelemetry span tags. |

`AddMessageForgeRabbitMQ` registers the `MessageForge` activity source with OpenTelemetry tracing. Pair it with your existing OpenTelemetry exporter configuration.

```csharp
options.IncludeMessageContentInOpenTelemetry();
```

### Lifecycle hooks

Hooks are invoked in registration order (FIFO). Built-in logging hooks are registered automatically; add your own for custom telemetry, metrics, or side effects.

| Method | When invoked |
| --- | --- |
| `BeforeMessageServiceStart` | Before exchanges and queues are created and consumers start. |
| `AfterMessageServiceStarted` | After all subscribers have started. |
| `BeforeMessageServiceStop` | Before consumers are stopped. |
| `AfterMessageServiceStopped` | After all subscribers have stopped. |
| `BeforeMessagePublish` | Before a message is serialized and published. |
| `AfterMessagePublished` | After a message is successfully published. |
| `BeforeMessageHandle` | Before a subscriber's `HandleAsync` is invoked. |
| `AfterMessageHandled` | After a message is successfully handled and acknowledged. |
| `OnMessagePublishError` | When publishing fails (excluding serialization errors). |
| `OnMessageSerializeError` | When publish-time serialization fails. |
| `OnMessageDeserializeError` | When consume-time deserialization fails. |
| `OnMessageHandleError` | When a subscriber's `HandleAsync` throws. |
| `OnMessageRetry` | Before a failed message is requeued for retry. |
| `OnRetryLimitReached` | When a message has exhausted retries and will be dead-lettered. |
| `BeforeOutboxEnqueue` | Before a message is serialized and written to the outbox table. |
| `AfterOutboxEnqueued` | After a message is written to the outbox table. |
| `OnOutboxSerializeError` | When outbox enqueue-time serialization fails. |
| `BeforeOutboxDispatch` | Before an outbox message is published to the broker. |
| `AfterOutboxDispatched` | After an outbox message is successfully published to the broker. |
| `OnOutboxDispatchError` | When dispatching an outbox message to the broker fails. |

```csharp
options.OnMessageHandleError(ctx =>
{
    var logger = ctx.ServiceProvider.GetRequiredService<ILogger<Program>>();
    logger.LogError(ctx.Exception, "Failed to handle {MessageType}", ctx.MessageType.Name);
    return Task.CompletedTask;
});
```

Hook context types (`MessageServiceContext`, `MessagePublishContext`, `MessageHandleContext`, `MessageErrorContext`, `OutboxEnqueueContext`, `OutboxDispatchContext`, `OutboxErrorContext`) expose the service provider, message, message type, delivery count, retry/dead-letter flags, cancellation token, and the in-flight `Activity` when applicable.
