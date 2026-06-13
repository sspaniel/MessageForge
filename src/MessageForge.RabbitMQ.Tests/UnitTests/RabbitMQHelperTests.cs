using MessageForge.RabbitMQ.Helpers;
using MessageForge.RabbitMQ.Services;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class RabbitMQHelperTests
{
    [Test]
    public void DefaultQueueArgs_Contains_Expected_Entries()
    {
        // act
        var args = RabbitMQHelper.DefaultQueueArgs;

        // assert
        args.Keys.ShouldBe(["x-dead-letter-exchange", "x-queue-type"], ignoreOrder: true);
        args["x-dead-letter-exchange"].ShouldBe(MessageService.DeadLetterExchangeName);
        args["x-queue-type"].ShouldBe("quorum");
    }

    [Test]
    public void Naming_Constants_Match_Documented_Contract()
    {
        // assert
        MessageService.DeadLetterQueueName.ShouldBe("MessageForge.DeadLetter");
        MessageService.DeadLetterExchangeName.ShouldBe("MessageForge.DeadLetterExchange");
        MessageService.ErrorQueueName.ShouldBe("MessageForge.Errors");
    }
}
