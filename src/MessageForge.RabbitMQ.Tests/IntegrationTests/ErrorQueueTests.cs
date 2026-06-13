using System.Text.Json;
using MessageForge.Errors;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.ConnectionPools;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

/// <summary>
/// Verifies that consumer failures publish a <see cref="MessageForgeError"/> to the dedicated error queue
/// (<c>MessageForge.Errors</c>), even when nothing subscribes to <see cref="MessageForgeError"/>.
/// </summary>
public sealed class ErrorQueueTests
{
    private ServiceProvider _serviceProvider = null!;
    private MessageService _messageService = null!;
    private IPublisher _publisher = null!;
    private IConnectionPool _connectionPool = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUpAsync()
    {
        _serviceProvider = RabbitMqTestHelpers.BuildServiceProvider(options =>
        {
            options.Subscribe<ErrorQueueSubscriber, ErrorQueueMessage>(subscriber =>
                subscriber.Retries(maxRetryCount: 1, retryDelay: TimeSpan.FromMilliseconds(50)));

            options.Subscribe<NestedErrorSubscriber, NestedErrorMessage>(subscriber =>
                subscriber.Retries(maxRetryCount: 1, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        _publisher = _serviceProvider.GetRequiredService<IPublisher>();
        _connectionPool = _serviceProvider.GetRequiredService<IConnectionPool>();
        var options = _serviceProvider.GetRequiredService<MessageServiceOptions>();
        _messageService = new MessageService(_serviceProvider, options, _connectionPool);
        await _messageService.StartAsync(CancellationToken.None);
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDownAsync()
    {
        try
        {
            await _messageService.StopAsync(CancellationToken.None);
        }
        catch
        {
        }

        _connectionPool?.Dispose();
        await _serviceProvider.DisposeAsync();
    }

    [Test]
    public async Task Failed_Message_Publishes_Error_To_Error_Queue()
    {
        // arrange
        var message = new ErrorQueueMessage { Guid = Guid.NewGuid() };
        var marker = $"error-marker-{message.Guid}";

        // act
        await _publisher.PublishAsync(message);

        var errors = await RabbitMqTestHelpers.DrainMatchingAsync(
            _connectionPool.GetConnection(),
            MessageService.ErrorQueueName,
            body => body.Contains(marker, StringComparison.Ordinal),
            expectedCount: 1,
            timeout: TimeSpan.FromSeconds(15));

        // assert
        errors.Count.ShouldBe(1);

        var error = JsonSerializer.Deserialize<MessageForgeError>(errors[0]);
        error.ShouldNotBeNull();
        error.ConsumerName.ShouldBe(nameof(ErrorQueueSubscriber));
        error.Message.ShouldBe(marker);
        error.StackTrace.ShouldNotBeNullOrWhiteSpace();
    }

    [Test]
    public async Task Nested_Exception_Is_Captured_In_Error_InnerError()
    {
        // arrange
        var message = new NestedErrorMessage { Guid = Guid.NewGuid() };
        var outerMarker = $"outer-{message.Guid}";
        var innerMarker = $"inner-{message.Guid}";

        // act
        await _publisher.PublishAsync(message);

        var errors = await RabbitMqTestHelpers.DrainMatchingAsync(
            _connectionPool.GetConnection(),
            MessageService.ErrorQueueName,
            body => body.Contains(outerMarker, StringComparison.Ordinal),
            expectedCount: 1,
            timeout: TimeSpan.FromSeconds(15));

        // assert
        errors.Count.ShouldBe(1);

        var error = JsonSerializer.Deserialize<MessageForgeError>(errors[0]);
        error.ShouldNotBeNull();
        error.Message.ShouldBe(outerMarker);
        error.InnerError.ShouldNotBeNull();
        error.InnerError.Message.ShouldBe(innerMarker);
    }
}
