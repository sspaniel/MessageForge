#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning disable CA1822 // Mark members as static
#pragma warning disable IDE0060 // Remove unused parameter

using System.Collections.Concurrent;
using MessageForge.Subscribers;

namespace MessageForge.RabbitMQ.Tests.TestObjects;

/// <summary>
/// Handler always throws so the message is retried and ultimately dead-lettered.
/// </summary>
public sealed class DeadLetterSubscriber : ISubscriber<DeadLetterMessage>
{
    private static int _attempts;

    public static int Attempts => _attempts;

    public static void Reset() => Interlocked.Exchange(ref _attempts, 0);

    public async Task HandleAsync(DeadLetterMessage message, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _attempts);
        throw new InvalidOperationException("Always fails.");
    }
}

/// <summary>
/// Used to verify that a subscriber configured with zero retries never processes the message.
/// </summary>
public sealed class ImmediateRejectSubscriber : ISubscriber<ImmediateRejectMessage>
{
    private static int _attempts;

    public static int Attempts => _attempts;

    public static void Reset() => Interlocked.Exchange(ref _attempts, 0);

    public async Task HandleAsync(ImmediateRejectMessage message, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _attempts);
        throw new InvalidOperationException("Always fails.");
    }
}

/// <summary>
/// Used to verify malformed payloads never reach the handler regardless of serializer behavior.
/// </summary>
public sealed class SerializerSubscriber :
    ISubscriber<SerializerDeadLetterMessage>,
    ISubscriber<SerializerIgnoreMessage>
{
    private static int _deadLetterHandled;
    private static int _ignoreHandled;

    public static int DeadLetterHandled => _deadLetterHandled;

    public static int IgnoreHandled => _ignoreHandled;

    public static void Reset()
    {
        Interlocked.Exchange(ref _deadLetterHandled, 0);
        Interlocked.Exchange(ref _ignoreHandled, 0);
    }

    public async Task HandleAsync(SerializerDeadLetterMessage message, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _deadLetterHandled);
    }

    public async Task HandleAsync(SerializerIgnoreMessage message, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _ignoreHandled);
    }
}

public sealed class FanoutSubscriberA : ISubscriber<FanoutMessage>
{
    private static readonly ConcurrentBag<Guid> _received = [];

    public static IReadOnlyCollection<Guid> Received => _received;

    public static void Reset() => _received.Clear();

    public async Task HandleAsync(FanoutMessage message, CancellationToken cancellationToken)
    {
        _received.Add(message.Guid);
    }
}

public sealed class FanoutSubscriberB : ISubscriber<FanoutMessage>
{
    private static readonly ConcurrentBag<Guid> _received = [];

    public static IReadOnlyCollection<Guid> Received => _received;

    public static void Reset() => _received.Clear();

    public async Task HandleAsync(FanoutMessage message, CancellationToken cancellationToken)
    {
        _received.Add(message.Guid);
    }
}

/// <summary>
/// Records receipts so a test can assert that TTL-expired messages are never delivered.
/// </summary>
public sealed class TtlSubscriber : ISubscriber<TtlMessage>
{
    private static int _received;

    public static int Received => _received;

    public static void Reset() => Interlocked.Exchange(ref _received, 0);

    public async Task HandleAsync(TtlMessage message, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _received);
    }
}

/// <summary>
/// Records receipts so a test can assert that overflowed messages are never delivered.
/// </summary>
public sealed class MaxLengthSubscriber : ISubscriber<MaxLengthMessage>
{
    private static int _received;

    public static int Received => _received;

    public static void Reset() => Interlocked.Exchange(ref _received, 0);

    public async Task HandleAsync(MaxLengthMessage message, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _received);
    }
}

public sealed class LifecycleSubscriber : ISubscriber<LifecycleMessage>
{
    private static readonly ConcurrentBag<Guid> _received = [];

    public static IReadOnlyCollection<Guid> Received => _received;

    public static void Reset() => _received.Clear();

    public async Task HandleAsync(LifecycleMessage message, CancellationToken cancellationToken)
    {
        _received.Add(message.Guid);
    }
}

/// <summary>
/// Records receipts so a test can assert that a null-deserialized message is acked and never handled.
/// </summary>
public sealed class NullMessageSubscriber : ISubscriber<NullableMessage>
{
    private static int _received;

    public static int Received => _received;

    public static void Reset() => Interlocked.Exchange(ref _received, 0);

    public async Task HandleAsync(NullableMessage message, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _received);
    }
}

/// <summary>
/// Delays while tracking the maximum observed concurrency to verify the prefetch limit is respected.
/// </summary>
public sealed class ConcurrencySubscriber : ISubscriber<ConcurrencyMessage>
{
    private static readonly ConcurrentBag<Guid> _received = [];
    private static int _current;
    private static int _maxObserved;

    public static IReadOnlyCollection<Guid> Received => _received;

    public static int MaxObservedConcurrency => _maxObserved;

    public static void Reset()
    {
        _received.Clear();
        Interlocked.Exchange(ref _current, 0);
        Interlocked.Exchange(ref _maxObserved, 0);
    }

    public async Task HandleAsync(ConcurrencyMessage message, CancellationToken cancellationToken)
    {
        var current = Interlocked.Increment(ref _current);

        int observed;
        do
        {
            observed = _maxObserved;
            if (current <= observed)
            {
                break;
            }
        }
        while (Interlocked.CompareExchange(ref _maxObserved, current, observed) != observed);

        try
        {
            await Task.Delay(100, cancellationToken);
            _received.Add(message.Guid);
        }
        finally
        {
            Interlocked.Decrement(ref _current);
        }
    }
}
