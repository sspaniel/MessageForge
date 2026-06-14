using MessageForge.Persistence.Outbox;
using MessageForge.Persistence.Services;
using MessageForge.RabbitMQ.Services;

namespace MessageForge.RabbitMQ.DependencyInjection;

/// <summary>
/// Outbox configuration extensions for <see cref="MessageServiceOptions"/>.
/// </summary>
public static class OutboxExtensions
{
    /// <summary>
    /// Enables and configures the transactional outbox for the specified <see cref="MessageForgeOutboxDbContext"/>.
    /// </summary>
    /// <typeparam name="TDbContext">The application database context type.</typeparam>
    /// <param name="options">The message service options.</param>
    /// <param name="configure">Optional action to configure outbox options.</param>
    public static void UseOutbox<TDbContext>(this MessageServiceOptions options, Action<OutboxOptions>? configure = null)
        where TDbContext : MessageForgeOutboxDbContext
    {
        options.ConfigureOutbox(outboxOptions =>
        {
            outboxOptions.ForDbContext<TDbContext>();
            configure?.Invoke(outboxOptions);
        });
    }
}
