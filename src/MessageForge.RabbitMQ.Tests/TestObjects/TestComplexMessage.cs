namespace MessageForge.RabbitMQ.Tests.TestObjects;

public sealed class TestComplexMessage
{
    public Guid Guid { get; set; }

    public IEnumerable<TestSimpleMessage> SimpleMessages { get; set; } = [];
}
