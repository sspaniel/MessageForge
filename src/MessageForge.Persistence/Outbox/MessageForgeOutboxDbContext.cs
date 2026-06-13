using Microsoft.EntityFrameworkCore;

namespace MessageForge.Persistence.Outbox;

/// <summary>
/// Base <see cref="DbContext"/> that provides the transactional outbox <see cref="DbSet{OutboxMessage}"/>.
/// Application contexts must inherit from this type to use the outbox.
/// </summary>
public abstract class MessageForgeOutboxDbContext : DbContext
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MessageForgeOutboxDbContext"/> class.
    /// </summary>
    /// <param name="options">The options for this context.</param>
    protected MessageForgeOutboxDbContext(DbContextOptions options)
        : base(options)
    {
    }

    /// <summary>
    /// Gets the outbox messages pending dispatch.
    /// </summary>
    public DbSet<OutboxMessage> OutboxMessages => Set<OutboxMessage>();

    /// <inheritdoc />
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.ConfigureOutbox();
        base.OnModelCreating(modelBuilder);
    }
}
