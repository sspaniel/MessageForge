using MessageForge.Persistence.Outbox;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageForge.Persistence.Services;

internal sealed class OutboxService : BackgroundService
{
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OutboxOptions _outboxOptions;
    private readonly ILogger<OutboxService> _logger;

    public OutboxService(
        IServiceScopeFactory scopeFactory,
        OutboxOptions outboxOptions,
        ILogger<OutboxService> logger)
    {
        _scopeFactory = scopeFactory;
        _outboxOptions = outboxOptions;
        _logger = logger;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var hasBacklog = false;

            try
            {
                hasBacklog = await ProcessBatchAsync(stoppingToken);
            }
            catch (Exception error) when (error is not OperationCanceledException)
            {
                _logger.LogError(error, "Error processing outbox batch.");
            }

            if (hasBacklog)
            {
                continue;
            }

            await Task.Delay(_outboxOptions.PollingInterval, stoppingToken);
        }
    }

    private async Task<bool> ProcessBatchAsync(CancellationToken cancellationToken)
    {
        await using var scope = _scopeFactory.CreateAsyncScope();
        var dispatcher = scope.ServiceProvider.GetRequiredService<IOutboxDispatcher>();
        var dbContext = (MessageForgeOutboxDbContext)scope.ServiceProvider.GetRequiredService(_outboxOptions.DbContextType);

        var batch = await dbContext.OutboxMessages
            .AsNoTracking()
            .OrderBy(message => message.Sequence)
            .Take(_outboxOptions.BatchSize)
            .ToListAsync(cancellationToken);

        if (batch.Count == 0)
        {
            return false;
        }

        var dispatchedIds = new List<Guid>(batch.Count);

        foreach (var message in batch)
        {
            await dispatcher.DispatchAsync(message.MessageType, message.Payload, cancellationToken);
            dispatchedIds.Add(message.Id);
        }

        if (dispatchedIds.Count > 0)
        {
            await dbContext.OutboxMessages
                .Where(message => dispatchedIds.Contains(message.Id))
                .ExecuteDeleteAsync(cancellationToken);
        }

        return batch.Count >= _outboxOptions.BatchSize;
    }
}
