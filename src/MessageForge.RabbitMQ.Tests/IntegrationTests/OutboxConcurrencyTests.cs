using MessageForge.Persistence.Outbox;
using MessageForge.Persistence.Services;
using MessageForge.Persistence.UnitOfWork;
using MessageForge.Publishers;
using MessageForge.RabbitMQ.DependencyInjection;
using MessageForge.RabbitMQ.Services;
using MessageForge.RabbitMQ.Tests.TestObjects;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;

namespace MessageForge.RabbitMQ.Tests.IntegrationTests;

[Parallelizable(ParallelScope.None)]
public sealed class OutboxConcurrencyTests
{
    private const int MultiDispatcherHostCount = 3;
    private const int MultiDispatcherMessageCount = 30;

    [SetUp]
    public void TestSetUp()
    {
        OutboxMultiDispatchTestSubscriber.Reset();
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Multiple_Dispatchers_Deliver_Each_Message_Once(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);
        await MultiDispatcher_Delivers_Each_Message_OnceAsync(connectionString, configureDbContext);
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Crashed_Dispatcher_Orphaned_Lease_Expiry_Delivers_Each_Message_Once(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);
        await CrashedDispatcher_Orphaned_Lease_Expiry_Delivers_Each_Message_OnceAsync(connectionString, configureDbContext);
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Reclaims_Messages_With_Expired_Lease(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);
        var messageId = Guid.NewGuid();

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);
            options.UseOutbox<TestOutboxDbContext>(outbox =>
            {
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100));
                outbox.WithLeaseDuration(TimeSpan.FromSeconds(30));
            });

            options.Subscribe<OutboxMultiDispatchTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        using var host = builder.Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(host.Services);

        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
            dbContext.OutboxMessages.Add(new OutboxMessage
            {
                Id = messageId,
                MessageType = typeof(OutboxMultiDispatchTestMessage).FullName!,
                Payload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(new OutboxMultiDispatchTestMessage { Id = messageId }),
                CreatedAt = DateTimeOffset.UtcNow,
                LockedUntil = DateTimeOffset.UtcNow.AddMinutes(-1),
                LockedBy = "stale-worker",
            });

            await dbContext.SaveChangesAsync();
        }

        await host.StartAsync();

        try
        {
            (await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxMultiDispatchTestSubscriber.ReceivedIds.Contains(messageId),
                TimeSpan.FromSeconds(15))).ShouldBeTrue();

            var removed = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = host.Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any(message => message.Id == messageId);
            }, TimeSpan.FromSeconds(15));

            removed.ShouldBeTrue();
            OutboxMultiDispatchTestSubscriber.GetReceiveCount(messageId).ShouldBe(1);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Releases_Lease_After_Dispatch_Failure_Then_Retries(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);
        var messageId = Guid.NewGuid();

        using var host = await CreateAndStartOutboxHostAsync(
            connectionString,
            configureDbContext,
            configureOutbox: outbox =>
            {
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100));
                outbox.WithLeaseDuration(TimeSpan.FromSeconds(5));
            },
            configure: options => options.Subscribe<OutboxMultiDispatchTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50))));

        try
        {
            await RabbitMqSharedFixture.PauseAsync();

            try
            {
                using (var scope = host.Services.CreateScope())
                {
                    var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                    var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                    await unitOfWork.ExecuteAsync(async ct => await publisher.PublishAsync(
                        new OutboxMultiDispatchTestMessage { Id = messageId },
                        ct));
                }

                var leaseReleased = await RabbitMqTestHelpers.WaitForAsync(() =>
                {
                    using var scope = host.Services.CreateScope();
                    var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                    var message = dbContext.OutboxMessages.SingleOrDefault(m => m.Id == messageId);
                    return message is not null && message.LockedBy is null && message.LockedUntil is null;
                }, TimeSpan.FromSeconds(60));

                leaseReleased.ShouldBeTrue();
            }
            finally
            {
                await RabbitMqSharedFixture.UnpauseAsync();
            }

            (await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxMultiDispatchTestSubscriber.ReceivedIds.Contains(messageId),
                TimeSpan.FromSeconds(60))).ShouldBeTrue();

            OutboxMultiDispatchTestSubscriber.GetReceiveCount(messageId).ShouldBe(1);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static async Task MultiDispatcher_Delivers_Each_Message_OnceAsync(
        string connectionString,
        Action<DbContextOptionsBuilder, string> configureDbContext)
    {
        var messageIds = Enumerable.Range(0, MultiDispatcherMessageCount)
            .Select(_ => Guid.NewGuid())
            .ToArray();

        using var schemaHost = CreateEnqueueHost(connectionString, configureDbContext).Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(schemaHost.Services);

        var dispatcherHosts = Enumerable.Range(0, MultiDispatcherHostCount)
            .Select(_ => CreateDispatcherHostBuilder(connectionString, configureDbContext).Build())
            .ToArray();

        await Task.WhenAll(dispatcherHosts.Select(host => host.StartAsync()));

        // Allow message services to declare exchanges and bind subscriber queues before dispatching.
        await Task.Delay(TimeSpan.FromSeconds(2));

        using (var enqueueHost = CreateEnqueueHost(connectionString, configureDbContext).Build())
        {
            using var scope = enqueueHost.Services.CreateScope();
            var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

            foreach (var messageId in messageIds)
            {
                await unitOfWork.ExecuteAsync(async ct => await publisher.PublishAsync(
                    new OutboxMultiDispatchTestMessage { Id = messageId },
                    ct));
            }
        }

        try
        {
            (await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxMultiDispatchTestSubscriber.ReceivedIds.Count == MultiDispatcherMessageCount,
                TimeSpan.FromSeconds(60))).ShouldBeTrue();

            OutboxMultiDispatchTestSubscriber.AssertEachReceivedExactlyOnce(messageIds);

            var removed = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = dispatcherHosts[0].Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any();
            }, TimeSpan.FromSeconds(15));

            removed.ShouldBeTrue();
        }
        finally
        {
            await Task.WhenAll(dispatcherHosts.Select(host => host.StopAsync()));
        }
    }

    /// <summary>
    /// Simulates a dispatcher crash after claiming a batch but before publishing completes.
    /// Rows remain leased until expiry, then surviving dispatchers reclaim and publish once each.
    /// </summary>
    private static async Task CrashedDispatcher_Orphaned_Lease_Expiry_Delivers_Each_Message_OnceAsync(
        string connectionString,
        Action<DbContextOptionsBuilder, string> configureDbContext)
    {
        const int messageCount = 12;
        const int orphanedLeaseSeconds = 3;

        var messageIds = Enumerable.Range(0, messageCount)
            .Select(_ => Guid.NewGuid())
            .ToArray();

        using var schemaHost = CreateEnqueueHost(connectionString, configureDbContext, orphanedLeaseSeconds).Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(schemaHost.Services);

        using (var enqueueHost = CreateEnqueueHost(connectionString, configureDbContext, orphanedLeaseSeconds).Build())
        {
            using var scope = enqueueHost.Services.CreateScope();
            var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
            var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
            var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();

            foreach (var messageId in messageIds)
            {
                await unitOfWork.ExecuteAsync(async ct => await publisher.PublishAsync(
                    new OutboxMultiDispatchTestMessage { Id = messageId },
                    ct));
            }

            // Simulate a dispatcher that claimed a batch and crashed before publishing or releasing leases.
            var orphanedLeaseUntil = DateTimeOffset.UtcNow.AddSeconds(orphanedLeaseSeconds);
            var outboxRows = await dbContext.OutboxMessages.ToListAsync();
            outboxRows.Count.ShouldBe(messageCount);

            foreach (var entity in outboxRows)
            {
                entity.LockedUntil = orphanedLeaseUntil;
                entity.LockedBy = "crashed-dispatcher";
            }

            await dbContext.SaveChangesAsync();
        }

        var dispatcherHosts = Enumerable.Range(0, MultiDispatcherHostCount)
            .Select(_ => CreateDispatcherHostBuilder(connectionString, configureDbContext, orphanedLeaseSeconds).Build())
            .ToArray();

        await Task.WhenAll(dispatcherHosts.Select(host => host.StartAsync()));

        try
        {
            (await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxMultiDispatchTestSubscriber.ReceivedIds.Count == messageCount,
                TimeSpan.FromSeconds(30))).ShouldBeTrue();

            OutboxMultiDispatchTestSubscriber.AssertEachReceivedExactlyOnce(messageIds);

            var removed = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = dispatcherHosts[0].Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any();
            }, TimeSpan.FromSeconds(15));

            removed.ShouldBeTrue();
        }
        finally
        {
            await Task.WhenAll(dispatcherHosts.Select(host => host.StopAsync()));
        }
    }

    private static HostApplicationBuilder CreateEnqueueHost(
        string connectionString,
        Action<DbContextOptionsBuilder, string> configureDbContext,
        int leaseDurationSeconds = 30)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);
            options.UseOutbox<TestOutboxDbContext>(outbox =>
            {
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100));
                outbox.WithBatchSize(10);
                outbox.WithDispatchConcurrency(4);
                outbox.WithLeaseDuration(TimeSpan.FromSeconds(leaseDurationSeconds));
            });
        });

        return builder;
    }

    private static HostApplicationBuilder CreateDispatcherHostBuilder(
        string connectionString,
        Action<DbContextOptionsBuilder, string> configureDbContext,
        int leaseDurationSeconds = 30)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);
            options.UseOutbox<TestOutboxDbContext>(outbox =>
            {
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100));
                outbox.WithBatchSize(10);
                outbox.WithDispatchConcurrency(4);
                outbox.WithLeaseDuration(TimeSpan.FromSeconds(leaseDurationSeconds));
            });

            options.Subscribe<OutboxMultiDispatchTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        return builder;
    }

    private static async Task<IHost> CreateAndStartOutboxHostAsync(
        string connectionString,
        Action<DbContextOptionsBuilder, string> configureDbContext,
        Action<OutboxOptions>? configureOutbox = null,
        Action<MessageServiceOptions>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);
            options.UseOutbox<TestOutboxDbContext>(outbox =>
            {
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100));
                configureOutbox?.Invoke(outbox);
            });

            configure?.Invoke(options);
        });

        var host = builder.Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(host.Services);
        await host.StartAsync();
        return host;
    }
}
