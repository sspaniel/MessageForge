namespace MessageForge.Subscribers;

/// <summary>
/// Specifies the behavior for handling exceptions thrown by the serializer.
/// </summary>
public enum SubscriberSerializerExceptionBehavior
{
    /// <summary>
    /// Ignore the exception and continue.
    /// </summary>
    Ignore,

    /// <summary>
    /// Move the message to the dead letter.
    /// </summary>
    DeadLetter,
}
