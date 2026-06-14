using System.Collections.Concurrent;
using MessageForge.Persistence.Outbox;
using MessageForge.Persistence.Outbox.Lifecycle;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace MessageForge.Persistence.Services;

internal sealed class OutboxService : BackgroundService
{
    private static readonly ConcurrentDictionary<string, Type> MessageTypes = new(StringComparer.Ordinal);

    private readonly IServiceScopeFactory _scopeFactory;
    private readonly OutboxOptions _outboxOptions;
    private readonly IOutboxWorkerId _workerId;
    private readonly ILogger<OutboxService> _logger;
    private DateTimeOffset _lastPurgeAt = DateTimeOffset.MinValue;

    public OutboxService(
        IServiceScopeFactory scopeFactory,
        OutboxOptions outboxOptions,
        IOutboxWorkerId workerId,
        ILogger<OutboxService> logger)
    {
        _scopeFactory = scopeFactory;
        _outboxOptions = outboxOptions;
        _workerId = workerId;
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
        var outboxMessageStore = scope.ServiceProvider.GetRequiredService<IOutboxMessageStore>();
        var dbContext = (MessageForgeOutboxDbContext)scope.ServiceProvider.GetRequiredService(_outboxOptions.DbContextType);

        await PurgeExpiredMessagesAsync(dbContext, cancellationToken);

        var batch = await outboxMessageStore.ClaimBatchAsync(
            _workerId.Value,
            _outboxOptions.BatchSize,
            _outboxOptions.LeaseDuration,
            cancellationToken);

        if (batch.Count == 0)
        {
            return false;
        }

        var dispatchedIds = new List<Guid>(batch.Count);
        var releaseIds = new List<Guid>();

        if (_outboxOptions.DispatchConcurrency == 1)
        {
            foreach (var message in batch)
            {
                await DispatchMessageAsync(
                    scope.ServiceProvider,
                    dispatcher,
                    message,
                    dispatchedIds,
                    releaseIds,
                    cancellationToken);
            }
        }
        else
        {
            var resultsLock = new object();

            await Parallel.ForEachAsync(
                batch,
                new ParallelOptions
                {
                    MaxDegreeOfParallelism = _outboxOptions.DispatchConcurrency,
                    CancellationToken = cancellationToken,
                },
                async (message, ct) =>
                {
                    var (dispatched, release) = await TryDispatchMessageAsync(
                        scope.ServiceProvider,
                        dispatcher,
                        message,
                        ct);

                    lock (resultsLock)
                    {
                        if (dispatched)
                        {
                            dispatchedIds.Add(message.Id);
                        }
                        else if (release)
                        {
                            releaseIds.Add(message.Id);
                        }
                    }
                });
        }

        await outboxMessageStore.CompleteBatchAsync(
            _workerId.Value,
            dispatchedIds,
            releaseIds,
            cancellationToken);

        return batch.Count >= _outboxOptions.BatchSize;
    }

    private async Task DispatchMessageAsync(
        IServiceProvider serviceProvider,
        IOutboxDispatcher dispatcher,
        OutboxClaimedMessage message,
        List<Guid> dispatchedIds,
        List<Guid> releaseIds,
        CancellationToken cancellationToken)
    {
        var (dispatched, release) = await TryDispatchMessageAsync(
            serviceProvider,
            dispatcher,
            message,
            cancellationToken);

        if (dispatched)
        {
            dispatchedIds.Add(message.Id);
        }
        else if (release)
        {
            releaseIds.Add(message.Id);
        }
    }

    private async Task<(bool Dispatched, bool Release)> TryDispatchMessageAsync(
        IServiceProvider serviceProvider,
        IOutboxDispatcher dispatcher,
        OutboxClaimedMessage message,
        CancellationToken cancellationToken)
    {
        OutboxDispatchContext? dispatchContext = null;

        try
        {
            dispatchContext = new OutboxDispatchContext
            {
                ServiceProvider = serviceProvider,
                OutboxMessageId = message.Id,
                MessageType = message.MessageType,
                Payload = message.Payload,
                CancellationToken = cancellationToken,
            };

            await OutboxOptions.InvokeHooksAsync(_outboxOptions.BeforeOutboxDispatchHooks, dispatchContext);
            await dispatcher.DispatchAsync(message.MessageType, message.Payload, cancellationToken);
            await OutboxOptions.InvokeHooksAsync(_outboxOptions.AfterOutboxDispatchedHooks, dispatchContext);
            return (true, false);
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            _logger.LogError(
                error,
                "Failed to dispatch outbox message {OutboxMessageId} of type {MessageType}.",
                message.Id,
                message.MessageType);

            await OutboxOptions.InvokeHooksAsync(
                _outboxOptions.OnOutboxDispatchErrorHooks,
                new OutboxErrorContext
                {
                    ServiceProvider = serviceProvider,
                    MessageType = ResolveMessageType(message.MessageType),
                    OutboxMessageId = message.Id,
                    DispatchedMessageType = message.MessageType,
                    Payload = message.Payload,
                    Exception = error,
                    CancellationToken = cancellationToken,
                    Activity = dispatchContext?.Activity,
                });

            return (false, true);
        }
    }

    private async Task PurgeExpiredMessagesAsync(
        MessageForgeOutboxDbContext dbContext,
        CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;

        if (now - _lastPurgeAt < _outboxOptions.PurgeInterval)
        {
            return;
        }

        _lastPurgeAt = now;
        var retentionCutoff = now - _outboxOptions.RetentionPeriod;

        await dbContext.OutboxMessages
            .Where(message => message.CreatedAt < retentionCutoff)
            .ExecuteDeleteAsync(cancellationToken);
    }

    private static Type ResolveMessageType(string messageType) =>
        MessageTypes.GetOrAdd(messageType, static name => Type.GetType(name) ?? typeof(object));
}
