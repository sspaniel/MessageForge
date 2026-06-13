using MessageForge.RabbitMQ.Publishers;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class PublisherOptionsValidationTests
{
    [Test]
    public void Validate_Does_Not_Throw()
    {
        // arrange
        var options = new PublisherOptions();

        // act / assert
        Should.NotThrow(() => options.Validate());
    }
}
