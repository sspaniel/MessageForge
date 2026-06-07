#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
#pragma warning disable CA1822 // Mark members as static
#pragma warning disable IDE0060 // Remove unused parameter

using System.Collections.Concurrent;
using MessageForge.Subscribers;

namespace MessageForge.RabbitMQ.Tests.TestObjects;

public sealed class HostedSubscriber : ISubscriber<HostedMessage>
{
    private static readonly ConcurrentBag<Guid> _received = [];

    public static IReadOnlyCollection<Guid> Received => _received;

    public static void Reset() => _received.Clear();

    public async Task HandleAsync(HostedMessage message, CancellationToken cancellationToken)
    {
        _received.Add(message.Guid);
    }
}

/// <summary>
/// Always throws with a message containing the message guid so the resulting error can be located in the error queue.
/// </summary>
public sealed class ErrorQueueSubscriber : ISubscriber<ErrorQueueMessage>
{
    public async Task HandleAsync(ErrorQueueMessage message, CancellationToken cancellationToken)
    {
        throw new InvalidOperationException($"error-marker-{message.Guid}");
    }
}

/// <summary>
/// Always throws an exception that wraps an inner exception, both tagged with the message guid.
/// </summary>
public sealed class NestedErrorSubscriber : ISubscriber<NestedErrorMessage>
{
    public async Task HandleAsync(NestedErrorMessage message, CancellationToken cancellationToken)
    {
        throw new InvalidOperationException(
            $"outer-{message.Guid}",
            new ApplicationException($"inner-{message.Guid}"));
    }
}

public sealed class SerializerRoundtripSubscriber : ISubscriber<OptionalFieldsMessage>
{
    private static readonly ConcurrentBag<OptionalFieldsMessage> _received = [];

    public static IReadOnlyCollection<OptionalFieldsMessage> Received => _received;

    public static void Reset() => _received.Clear();

    public async Task HandleAsync(OptionalFieldsMessage message, CancellationToken cancellationToken)
    {
        _received.Add(message);
    }
}

public sealed class RecoverySubscriber : ISubscriber<RecoveryMessage>
{
    private static readonly ConcurrentBag<Guid> _received = [];

    public static IReadOnlyCollection<Guid> Received => _received;

    public static void Reset() => _received.Clear();

    public async Task HandleAsync(RecoveryMessage message, CancellationToken cancellationToken)
    {
        _received.Add(message.Guid);
    }
}
