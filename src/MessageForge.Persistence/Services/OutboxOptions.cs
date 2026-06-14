using MessageForge.Persistence.Outbox;
using MessageForge.Persistence.Outbox.Lifecycle;
using MessageForge.Publishers;

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

    /// <summary>
    /// Gets or sets how long undispatched outbox messages are retained before being deleted.
    /// </summary>
    public TimeSpan RetentionPeriod { get; set; } = TimeSpan.FromDays(30);

    /// <summary>
    /// Gets or sets how often expired outbox messages are purged.
    /// </summary>
    public TimeSpan PurgeInterval { get; set; } = TimeSpan.FromMinutes(15);

    /// <summary>
    /// Gets or sets the maximum number of outbox messages dispatched concurrently per instance.
    /// </summary>
    public int DispatchConcurrency { get; set; } = Math.Min(Environment.ProcessorCount, 16);

    /// <summary>
    /// Gets or sets how long a claimed outbox message remains locked before it can be reclaimed.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

    internal Type DbContextType { get; set; } = null!;

    internal bool EnableDeduplication { get; set; } = true;

    internal PublisherSerializerExceptionBehavior SerializerExceptionBehavior { get; set; }
        = PublisherSerializerExceptionBehavior.Ignore;

    internal LinkedList<Func<OutboxEnqueueContext, Task>> BeforeOutboxEnqueueHooks { get; } = new();

    internal LinkedList<Func<OutboxEnqueueContext, Task>> AfterOutboxEnqueuedHooks { get; } = new();

    internal LinkedList<Func<OutboxErrorContext, Task>> OnOutboxSerializeErrorHooks { get; } = new();

    internal LinkedList<Func<OutboxDispatchContext, Task>> BeforeOutboxDispatchHooks { get; } = new();

    internal LinkedList<Func<OutboxDispatchContext, Task>> AfterOutboxDispatchedHooks { get; } = new();

    internal LinkedList<Func<OutboxErrorContext, Task>> OnOutboxDispatchErrorHooks { get; } = new();

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
    /// Sets how long undispatched outbox messages are retained before being deleted.
    /// </summary>
    /// <param name="retentionPeriod">The retention period.</param>
    /// <returns>The current <see cref="OutboxOptions"/> instance.</returns>
    public OutboxOptions WithRetentionPeriod(TimeSpan retentionPeriod)
    {
        RetentionPeriod = retentionPeriod;
        return this;
    }

    /// <summary>
    /// Sets how often expired outbox messages are purged.
    /// </summary>
    /// <param name="purgeInterval">The purge interval.</param>
    /// <returns>The current <see cref="OutboxOptions"/> instance.</returns>
    public OutboxOptions WithPurgeInterval(TimeSpan purgeInterval)
    {
        PurgeInterval = purgeInterval;
        return this;
    }

    /// <summary>
    /// Sets the maximum number of outbox messages dispatched concurrently per instance.
    /// </summary>
    /// <param name="dispatchConcurrency">The dispatch concurrency.</param>
    /// <returns>The current <see cref="OutboxOptions"/> instance.</returns>
    public OutboxOptions WithDispatchConcurrency(int dispatchConcurrency)
    {
        DispatchConcurrency = dispatchConcurrency;
        return this;
    }

    /// <summary>
    /// Sets how long a claimed outbox message remains locked before it can be reclaimed.
    /// </summary>
    /// <param name="leaseDuration">The lease duration.</param>
    /// <returns>The current <see cref="OutboxOptions"/> instance.</returns>
    public OutboxOptions WithLeaseDuration(TimeSpan leaseDuration)
    {
        LeaseDuration = leaseDuration;
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

        if (RetentionPeriod <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(RetentionPeriod));
        }

        if (PurgeInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(PurgeInterval));
        }

        if (DispatchConcurrency < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(DispatchConcurrency));
        }

        if (LeaseDuration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(LeaseDuration));
        }
    }

    internal static async Task InvokeHooksAsync<TContext>(
        IEnumerable<Func<TContext, Task>> hooks,
        TContext context)
    {
        foreach (var hook in hooks)
        {
            await hook(context);
        }
    }
}
