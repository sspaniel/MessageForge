namespace MessageForge.RabbitMQ.Tests.TestObjects;

public sealed class TestSimpleMessage
{
    public Guid Guid { get; set; }

    public string String { get; set; } = string.Empty;

    public int Integer { get; set; }

    public float Float { get; set; }

    public DateTime DateTime { get; set; }
}
