namespace MessageForge.RabbitMQ.Consumers;

internal interface IRabbitMQSubscriber
{
    Task InitializeAsync(CancellationToken cancellationToken = default);

    Task StartAsync(CancellationToken cancellationToken = default);

    Task StopAsync(CancellationToken cancellationToken = default);
}
