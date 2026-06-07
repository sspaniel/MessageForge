namespace MessageForge.RabbitMQ.Tests.TestObjects;

public sealed class DeadLetterMessage
{
    public Guid Guid { get; set; }
}

public sealed class ImmediateRejectMessage
{
    public Guid Guid { get; set; }
}

public sealed class SerializerDeadLetterMessage
{
    public Guid Guid { get; set; }
}

public sealed class SerializerIgnoreMessage
{
    public Guid Guid { get; set; }
}

public sealed class FanoutMessage
{
    public Guid Guid { get; set; }
}

public sealed class TtlMessage
{
    public Guid Guid { get; set; }
}

public sealed class MaxLengthMessage
{
    public Guid Guid { get; set; }
}

public sealed class LifecycleMessage
{
    public Guid Guid { get; set; }
}

public sealed class NullableMessage
{
    public Guid Guid { get; set; }
}

public sealed class NoSubscriberMessage
{
    public Guid Guid { get; set; }
}

public sealed class ConcurrencyMessage
{
    public Guid Guid { get; set; }
}

public sealed class CyclicMessage
{
    public Guid Guid { get; set; }

    public CyclicMessage? Self { get; set; }
}
