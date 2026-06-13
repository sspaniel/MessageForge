namespace MessageForge.RabbitMQ.Subscribers;

internal interface IRabbitMQSubscriber
{
    Task InitializeAsync(CancellationToken cancellationToken = default);

    Task StartAsync(CancellationToken cancellationToken = default);

    Task StopAsync(CancellationToken cancellationToken = default);
}
