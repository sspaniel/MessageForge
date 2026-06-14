using Microsoft.EntityFrameworkCore;

namespace MessageForge.Persistence.Outbox;

internal static class OutboxModelBuilderExtensions
{
    internal static void ConfigureOutbox(this ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<OutboxMessage>(entity =>
        {
            entity.ToTable("OutboxMessages", "MessageForge");
            entity.HasKey(message => message.Id);
            entity.Property(message => message.Sequence).ValueGeneratedOnAdd();
            entity.Property(message => message.MessageType).IsRequired();
            entity.Property(message => message.Payload).IsRequired();
            entity.Property(message => message.LockedBy).HasMaxLength(128);
            entity.HasIndex(message => message.Sequence);
            entity.HasIndex(message => message.CreatedAt);
            entity.HasIndex(message => new { message.LockedUntil, message.Sequence });
        });
    }
}
