using MessageForge.Errors;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class MessageForgeErrorTests
{
    [Test]
    public void Constructor_Maps_ConsumerName_Message_And_StackTrace()
    {
        // arrange
        var exception = CreateThrownException("boom");

        // act
        var error = new MessageForgeError("MyConsumer", exception);

        // assert
        error.ConsumerName.ShouldBe("MyConsumer");
        error.Message.ShouldBe("boom");
        error.StackTrace.ShouldNotBeNullOrEmpty();
    }

    [Test]
    public void Constructor_Sets_InnerError_Null_When_No_InnerException()
    {
        // arrange
        var exception = new InvalidOperationException("no inner");

        // act
        var error = new MessageForgeError("MyConsumer", exception);

        // assert
        error.InnerError.ShouldBeNull();
    }

    [Test]
    public void Constructor_Maps_Nested_InnerExceptions_Recursively()
    {
        // arrange
        var root = new ArgumentException("root cause");
        var middle = new InvalidOperationException("middle", root);
        var outer = new Exception("outer", middle);

        // act
        var error = new MessageForgeError("MyConsumer", outer);

        // assert
        error.Message.ShouldBe("outer");
        error.InnerError.ShouldNotBeNull();
        error.InnerError!.ConsumerName.ShouldBe("MyConsumer");
        error.InnerError.Message.ShouldBe("middle");
        error.InnerError.InnerError.ShouldNotBeNull();
        error.InnerError.InnerError!.Message.ShouldBe("root cause");
        error.InnerError.InnerError.InnerError.ShouldBeNull();
    }

    [Test]
    public void Constructor_Uses_Empty_String_When_StackTrace_Is_Null()
    {
        // arrange
        var exception = new InvalidOperationException("never thrown");

        // act
        var error = new MessageForgeError("MyConsumer", exception);

        // assert
        exception.StackTrace.ShouldBeNull();
        error.StackTrace.ShouldBe(string.Empty);
    }

    private static Exception CreateThrownException(string message)
    {
        try
        {
            throw new InvalidOperationException(message);
        }
        catch (InvalidOperationException exception)
        {
            return exception;
        }
    }
}
