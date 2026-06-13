using MessageForge.Persistence.Outbox;
using MessageForge.Persistence.Services;
using MessageForge.Persistence.UnitOfWork;
using MessageForge.Publishers;
using Microsoft.Extensions.DependencyInjection;

namespace MessageForge.Persistence.DependencyInjection;

/// <summary>
/// Dependency injection extensions for the transactional outbox.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers transactional outbox services.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="outboxOptions">The outbox options.</param>
    public static void AddMessageForgeOutbox(this IServiceCollection services, OutboxOptions outboxOptions)
    {
        ArgumentNullException.ThrowIfNull(outboxOptions);
        outboxOptions.Validate();

        services.AddSingleton(outboxOptions);
        services.AddSingleton<IOutboxMessageSerializer, JsonOutboxMessageSerializer>();

        var unitOfWorkType = typeof(EfUnitOfWork<>).MakeGenericType(outboxOptions.DbContextType);
        services.AddScoped(typeof(IUnitOfWork), unitOfWorkType);
        services.AddScoped<IPublisher, OutboxPublisher>();
        services.AddHostedService<OutboxService>();
    }
}
