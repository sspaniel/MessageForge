using System.Collections.Concurrent;
using MessageForge.Persistence.Outbox;
using MessageForge.Subscribers;
using Microsoft.EntityFrameworkCore;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.TestObjects;

public sealed class OutboxTestMessage
{
    public Guid Guid { get; set; }
}

public sealed class OutboxDeduplicatableTestMessage
{
    public Guid Id { get; set; }
}

public sealed class OutboxOrderedTestMessage
{
    public Guid Id { get; set; }

    public int Order { get; set; }
}

public sealed class OutboxMultiDispatchTestMessage
{
    public Guid Id { get; set; }
}

public sealed class OutboxMultiDispatchTestSubscriber : ISubscriber<OutboxMultiDispatchTestMessage>
{
    private static readonly ConcurrentBag<Guid> Received = [];

    public static IReadOnlyCollection<Guid> ReceivedIds => Received;

    public static int GetReceiveCount(Guid messageId) =>
        Received.Count(id => id == messageId);

    public static void Reset() => Received.Clear();

    public static void AssertEachReceivedExactlyOnce(IEnumerable<Guid> messageIds)
    {
        foreach (var messageId in messageIds)
        {
            GetReceiveCount(messageId).ShouldBe(1, customMessage: $"Message {messageId} should be received exactly once.");
        }
    }

    public Task HandleAsync(OutboxMultiDispatchTestMessage message, CancellationToken cancellationToken)
    {
        Received.Add(message.Id);
        return Task.CompletedTask;
    }
}

public sealed class OutboxTestSubscriber : ISubscriber<OutboxTestMessage>
{
    private static readonly ConcurrentBag<Guid> _received = [];

    public static IReadOnlyCollection<Guid> Received => _received;

    public static void Reset() => _received.Clear();

    public Task HandleAsync(OutboxTestMessage message, CancellationToken cancellationToken)
    {
        _received.Add(message.Guid);
        return Task.CompletedTask;
    }
}

public sealed class OutboxDeduplicatableTestSubscriber : ISubscriber<OutboxDeduplicatableTestMessage>
{
    private static readonly ConcurrentBag<Guid> _received = [];

    public static IReadOnlyCollection<Guid> Received => _received;

    public static void Reset() => _received.Clear();

    public Task HandleAsync(OutboxDeduplicatableTestMessage message, CancellationToken cancellationToken)
    {
        _received.Add(message.Id);
        return Task.CompletedTask;
    }
}

public sealed class OutboxOrderedTestSubscriber : ISubscriber<OutboxOrderedTestMessage>
{
    private static readonly object Sync = new();
    private static readonly List<int> ReceivedOrder = [];

    public static IReadOnlyList<int> GetReceivedOrder()
    {
        lock (Sync)
        {
            return [.. ReceivedOrder];
        }
    }

    public static void Reset()
    {
        lock (Sync)
        {
            ReceivedOrder.Clear();
        }
    }

    public Task HandleAsync(OutboxOrderedTestMessage message, CancellationToken cancellationToken)
    {
        lock (Sync)
        {
            ReceivedOrder.Add(message.Order);
        }

        return Task.CompletedTask;
    }
}

public sealed class TestOutboxDbContext : MessageForgeOutboxDbContext
{
    public TestOutboxDbContext(DbContextOptions<TestOutboxDbContext> options)
        : base(options)
    {
    }
}
