using MessageForge.Errors;
using MessageForge.Subscribers;

namespace MessageForge.RabbitMQ.Consumers;

/// <summary>
/// Options for configuring a message subscriber for RabbitMQ.
/// </summary>
/// <param name="subscriberType">The type of the subscriber.</param>
/// <param name="messageType">The type of the message the subscriber will handle.</param>
public sealed class SubscriberOptions(Type subscriberType, Type messageType)
{
    internal readonly Type SubscriberType = subscriberType ?? throw new ArgumentNullException(nameof(subscriberType));

    internal readonly Type MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));

    internal TimeSpan Ttl { get; set; } = TimeSpan.Zero;

    internal int MaxCount { get; set; } = 0;

    internal ushort MaxConcurrency { get; set; } = 10;

    internal int MaxRetryCount { get; set; } = 3;

    internal TimeSpan RetryDelay { get; set; } = TimeSpan.FromSeconds(1);

    internal SubscriberSerializerExceptionBehavior SerializerExceptionBehavior { get; set; }

    /// <summary>
    /// Validates the consumer options.
    /// </summary>
    public void Validate()
    {
        if (SubscriberType == null)
        {
            throw new ArgumentNullException(nameof(SubscriberType));
        }

        if (Ttl < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(Ttl));
        }

        if (MaxCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxCount));
        }

        if (MaxConcurrency < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxConcurrency));
        }

        if (MaxRetryCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxRetryCount));
        }

        if (RetryDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(RetryDelay));
        }

        if (MaxRetryCount > 0)
        {
            if (RetryDelay <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(RetryDelay));
            }
        }
    }

    /// <summary>
    /// Sets the time-to-live for messages in the queue. Once the TTL is reached, the message will be removed from the queue.
    /// </summary>
    /// <param name="messageTtl">Time-to-live for messages.</param>
    public void MessageTtl(TimeSpan messageTtl)
    {
        Ttl = messageTtl;
    }

    /// <summary>
    /// Sets the maximum number of messages that can be in the queue at any given time. Once the maximum is reached, no more messages will be accepted into the queue.
    /// </summary>
    /// <param name="maxCount">Maximum number of messages allowed in the queue.</param>
    public void MaxMessageCount(int maxCount)
    {
        MaxCount = maxCount;
    }

    /// <summary>
    /// Max number of messages that can be processed concurrently.
    /// </summary>
    /// <param name="maxConcurrency">Maximum number of messages that can be processed concurrently.</param>
    public void MaxMessageConcurrency(ushort maxConcurrency)
    {
        MaxConcurrency = maxConcurrency;
    }

    /// <summary>
    /// Sets the maximum number of times a message can be retried and the delay between each retry, before the message is moved to the dead-letter queue.
    /// </summary>
    /// <param name="maxRetryCount">Maximum number of times a message can be retried.</param>
    /// <param name="retryDelay">Delay between each retry.</param>
    public void Retries(int maxRetryCount, TimeSpan retryDelay)
    {
        MaxRetryCount = maxRetryCount;
        RetryDelay = retryDelay;
    }

    /// <summary>
    /// If the serializer encounters an exception while serializing a message, this behavior will be used.
    /// If set to <see cref="SubscriberSerializerExceptionBehavior.Ignore"/>, the exception will be ignored and the message will be removed from the queue.
    /// If set to <see cref="SubscriberSerializerExceptionBehavior.DeadLetter"/>, the message will be moved to the dead-letter queue and a <see cref="MessageForgeError"/> will be sent to the error queue.
    /// </summary>
    /// <param name="behavior">Behavior to use when a serializer exception occurs.</param>
    public void OnSerializationException(SubscriberSerializerExceptionBehavior behavior)
    {
        SerializerExceptionBehavior = behavior;
    }
}