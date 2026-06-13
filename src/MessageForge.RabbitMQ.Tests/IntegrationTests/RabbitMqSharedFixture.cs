using System.Diagnostics;
using System.Text;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Services;
using Microsoft.Extensions.DependencyInjection;
using RabbitMQ.Client;
using Testcontainers.RabbitMq;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

/// <summary>
/// Starts a single RabbitMQ container that is shared by every integration test in this namespace.
/// </summary>
[SetUpFixture]
public sealed class RabbitMqSharedFixture
{
    private static RabbitMqContainer _container = null!;

    public static string ConnectionString { get; private set; } = string.Empty;

    [OneTimeSetUp]
    public async Task GlobalSetUpAsync()
    {
        _container = new RabbitMqBuilder("rabbitmq:latest")
            .WithUsername("rabbitmq")
            .WithPassword("password")
            .Build();

        await _container.StartAsync();

        ConnectionString = $"amqp://rabbitmq:password@{_container.Hostname}:{_container.GetMappedPublicPort(5672)}/";
    }

    [OneTimeTearDown]
    public async Task GlobalTearDownAsync()
    {
        if (_container != null)
        {
            await _container.StopAsync();
            await _container.DisposeAsync();
        }
    }

    /// <summary>
    /// Pauses the shared broker to simulate a connection outage. Callers must ensure <see cref="UnpauseAsync"/>
    /// is invoked (for example in a finally block) so the container is restored for subsequent tests.
    /// </summary>
    public static Task PauseAsync() => _container.PauseAsync();

    public static Task UnpauseAsync() => _container.UnpauseAsync();
}

/// <summary>
/// Helpers shared by the integration tests for building service providers, publishing raw payloads,
/// polling for conditions, and draining queues (such as the dead-letter queue).
/// </summary>
public static class RabbitMqTestHelpers
{
    public static ServiceProvider BuildServiceProvider(Action<MessageServiceOptions> configure)
    {
        var services = new ServiceCollection();

        services
            .AddLogging()
            .AddMessageForgeRabbitMQ(options =>
            {
                options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);
                options.UseConnectionPoolSize(Environment.ProcessorCount);
                configure(options);
            });

        return services.BuildServiceProvider();
    }

    public static async Task<bool> WaitForAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(10);
        var stopwatch = Stopwatch.StartNew();

        while (!condition() && stopwatch.Elapsed < timeout)
        {
            await Task.Delay(50);
        }

        return condition();
    }

    public static async Task PublishRawAsync(IConnection connection, string exchange, ReadOnlyMemory<byte> body)
    {
        using var channel = await connection.CreateChannelAsync();

        await channel.BasicPublishAsync(
            exchange: exchange,
            routingKey: string.Empty,
            mandatory: false,
            body: body,
            basicProperties: new BasicProperties { Persistent = true });
    }

    public static async Task DeclareFanoutExchangeAsync(IConnection connection, string exchange)
    {
        using var channel = await connection.CreateChannelAsync();

        await channel.ExchangeDeclareAsync(
            exchange: exchange,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false);
    }

    /// <summary>
    /// Destructively reads (auto-acks) messages from the dead-letter queue, collecting those whose body matches
    /// the predicate, until <paramref name="expectedCount"/> matches are found or the timeout elapses.
    /// </summary>
    public static Task<List<string>> ReadDeadLetteredAsync(
        IConnection connection,
        Func<string, bool> predicate,
        int expectedCount,
        TimeSpan? timeout = null)
        => DrainMatchingAsync(connection, MessageService.DeadLetterQueueName, predicate, expectedCount, timeout);

    public static async Task<List<string>> DrainMatchingAsync(
        IConnection connection,
        string queueName,
        Func<string, bool> predicate,
        int expectedCount,
        TimeSpan? timeout = null)
    {
        timeout ??= TimeSpan.FromSeconds(10);
        var matches = new List<string>();
        var stopwatch = Stopwatch.StartNew();

        using var channel = await connection.CreateChannelAsync();

        while (matches.Count < expectedCount && stopwatch.Elapsed < timeout)
        {
            var result = await channel.BasicGetAsync(queueName, autoAck: true);

            if (result is null)
            {
                await Task.Delay(50);
                continue;
            }

            var body = Encoding.UTF8.GetString(result.Body.Span);

            if (predicate(body))
            {
                matches.Add(body);
            }
        }

        return matches;
    }
}
