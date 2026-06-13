using MessageForge.Persistence.Outbox;

namespace MessageForge.Persistence.Services;

/// <summary>
/// Options for the transactional outbox.
/// </summary>
public sealed class OutboxOptions
{
    /// <summary>
    /// Gets or sets how often the outbox service polls for unprocessed messages.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the maximum number of outbox messages dispatched per polling cycle.
    /// Increase this when draining large backlogs.
    /// </summary>
    public int BatchSize { get; set; } = 100;

    internal Type DbContextType { get; set; } = null!;

    internal bool EnableDeduplication { get; set; } = true;

    /// <summary>
    /// Configures the database context type used for the outbox.
    /// </summary>
    /// <typeparam name="TDbContext">The application database context type.</typeparam>
    /// <returns>The current <see cref="OutboxOptions"/> instance.</returns>
    public OutboxOptions ForDbContext<TDbContext>()
        where TDbContext : MessageForgeOutboxDbContext
    {
        DbContextType = typeof(TDbContext);
        return this;
    }

    /// <summary>
    /// Sets the maximum number of outbox messages dispatched per polling cycle.
    /// </summary>
    /// <param name="batchSize">The batch size.</param>
    /// <returns>The current <see cref="OutboxOptions"/> instance.</returns>
    public OutboxOptions WithBatchSize(int batchSize)
    {
        BatchSize = batchSize;
        return this;
    }

    /// <summary>
    /// Sets how often the outbox service polls for unprocessed messages.
    /// </summary>
    /// <param name="pollingInterval">The polling interval.</param>
    /// <returns>The current <see cref="OutboxOptions"/> instance.</returns>
    public OutboxOptions WithPollingInterval(TimeSpan pollingInterval)
    {
        PollingInterval = pollingInterval;
        return this;
    }

    /// <summary>
    /// Enables or disables outbox deduplication for pending messages.
    /// </summary>
    /// <param name="enabled">Whether deduplication is enabled.</param>
    /// <returns>The current <see cref="OutboxOptions"/> instance.</returns>
    public OutboxOptions WithDeduplication(bool enabled = true)
    {
        EnableDeduplication = enabled;
        return this;
    }

    /// <summary>
    /// Validates the outbox options.
    /// </summary>
    public void Validate()
    {
        if (DbContextType == null)
        {
            throw new InvalidOperationException("DbContext type must be configured.");
        }

        if (!typeof(MessageForgeOutboxDbContext).IsAssignableFrom(DbContextType))
        {
            throw new InvalidOperationException(
                $"{DbContextType.FullName} must inherit from {nameof(MessageForgeOutboxDbContext)}.");
        }

        if (BatchSize < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(BatchSize));
        }

        if (PollingInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(PollingInterval));
        }
    }
}
