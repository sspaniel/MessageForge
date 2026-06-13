using System.Text;
using System.Text.Json;
using MessageForge.RabbitMQ.Serializers;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Moq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.UnitTests;

public sealed class MessageSerializerTests
{
    [Test]
    public void Serialize_Then_Deserialize_Round_Trips_All_Fields()
    {
        // arrange
        var serializer = new MessageSerializer();
        var original = new TestComplexMessage
        {
            Guid = Guid.NewGuid(),
            SimpleMessages =
            [
                new TestSimpleMessage
                {
                    Guid = Guid.NewGuid(),
                    String = "hello",
                    Integer = 42,
                    Float = 3.5f,
                    DateTime = new DateTime(2026, 6, 13, 1, 2, 3, DateTimeKind.Utc),
                },
            ],
        };

        // act
        var bytes = serializer.Serialize(original);
        var result = serializer.Deserialize(typeof(TestComplexMessage), CreateEventArgs(bytes));

        // assert
        var roundTripped = result.ShouldBeOfType<TestComplexMessage>();
        roundTripped.Guid.ShouldBe(original.Guid);
        var simple = roundTripped.SimpleMessages.ShouldHaveSingleItem();
        var expected = original.SimpleMessages.Single();
        simple.Guid.ShouldBe(expected.Guid);
        simple.String.ShouldBe(expected.String);
        simple.Integer.ShouldBe(expected.Integer);
        simple.Float.ShouldBe(expected.Float);
        simple.DateTime.ShouldBe(expected.DateTime);
    }

    [Test]
    public void Serialize_Omits_Null_Properties()
    {
        // arrange
        var serializer = new MessageSerializer();
        var message = new NullableFieldMessage { Populated = "value", Optional = null };

        // act
        var json = Encoding.UTF8.GetString(serializer.Serialize(message));

        // assert
        json.ShouldContain(nameof(NullableFieldMessage.Populated));
        json.ShouldNotContain(nameof(NullableFieldMessage.Optional));
    }

    [Test]
    public void Serialize_Throws_When_Message_Is_Null()
    {
        // arrange
        var serializer = new MessageSerializer();

        // act / assert
        Should.Throw<ArgumentNullException>(() => serializer.Serialize<TestSimpleMessage>(null!));
    }

    [Test]
    public void Deserialize_Returns_Null_For_Null_Literal_Body()
    {
        // arrange
        var serializer = new MessageSerializer();
        var body = Encoding.UTF8.GetBytes("null");

        // act
        var result = serializer.Deserialize(typeof(TestSimpleMessage), CreateEventArgs(body));

        // assert
        result.ShouldBeNull();
    }

    [Test]
    public void Deserialize_Throws_For_Malformed_Json()
    {
        // arrange
        var serializer = new MessageSerializer();
        var body = Encoding.UTF8.GetBytes("{ not valid json");

        // act / assert
        Should.Throw<JsonException>(() => serializer.Deserialize(typeof(TestSimpleMessage), CreateEventArgs(body)));
    }

    private static BasicDeliverEventArgs CreateEventArgs(ReadOnlyMemory<byte> body) =>
        new(
            consumerTag: "consumer",
            deliveryTag: 1,
            redelivered: false,
            exchange: "exchange",
            routingKey: string.Empty,
            properties: Mock.Of<IReadOnlyBasicProperties>(),
            body: body);

    private sealed class NullableFieldMessage
    {
        public string Populated { get; set; } = string.Empty;

        public string? Optional { get; set; }
    }
}
