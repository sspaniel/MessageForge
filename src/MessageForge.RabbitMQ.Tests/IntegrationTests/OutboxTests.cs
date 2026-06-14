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
public sealed class OutboxTests
{
    [SetUp]
    public void TestSetUp()
    {
        OutboxTestSubscriber.Reset();
        OutboxDeduplicatableTestSubscriber.Reset();
        OutboxOrderedTestSubscriber.Reset();
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Commits_Message_And_Service_Delivers_To_Subscriber(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);

            options.UseOutbox<TestOutboxDbContext>(outbox =>
            {
                outbox.PollingInterval = TimeSpan.FromMilliseconds(100);
                outbox.BatchSize = 10;
            });

            options.Subscribe<OutboxTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        using var host = builder.Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(host.Services);
        await host.StartAsync();

        try
        {
            var message = new OutboxTestMessage { Guid = Guid.NewGuid() };

            using (var scope = host.Services.CreateScope())
            {
                var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                await unitOfWork.ExecuteAsync(async ct => await publisher.PublishAsync(message, ct));
            }

            await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxTestSubscriber.Received.Contains(message.Guid),
                TimeSpan.FromSeconds(15));

            var removed = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = host.Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any(outboxMessage => outboxMessage.MessageType == typeof(OutboxTestMessage).FullName);
            }, TimeSpan.FromSeconds(15));

            removed.ShouldBeTrue();
            OutboxTestSubscriber.Received.ShouldContain(message.Guid);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_ExecuteAsync_Works_With_EnableRetryOnFailure(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseWithRetryAsync(provider);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);

            options.UseOutbox<TestOutboxDbContext>(outbox =>
            {
                outbox.PollingInterval = TimeSpan.FromMilliseconds(100);
                outbox.BatchSize = 10;
            });

            options.Subscribe<OutboxTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        using var host = builder.Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(host.Services);
        await host.StartAsync();

        try
        {
            var message = new OutboxTestMessage { Guid = Guid.NewGuid() };

            using (var scope = host.Services.CreateScope())
            {
                var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                await unitOfWork.ExecuteAsync(async ct => await publisher.PublishAsync(message, ct));
            }

            await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxTestSubscriber.Received.Contains(message.Guid),
                TimeSpan.FromSeconds(15));

            OutboxTestSubscriber.Received.ShouldContain(message.Guid);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Deduplication_Skips_Duplicate_Pending_Messages(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);

            options.UseOutbox<TestOutboxDbContext>(outbox =>
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100)));

            options.Subscribe<OutboxDeduplicatableTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        using var host = builder.Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(host.Services);
        await host.StartAsync();

        try
        {
            var message = new OutboxDeduplicatableTestMessage { Id = Guid.NewGuid() };

            using (var scope = host.Services.CreateScope())
            {
                var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();

                await unitOfWork.ExecuteAsync(async ct =>
                {
                    await publisher.PublishAsync(message, ct);
                    await publisher.PublishAsync(message, ct);
                    dbContext.OutboxMessages.Local.Count.ShouldBe(1);
                });
            }

            await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxDeduplicatableTestSubscriber.Received.Count == 1,
                TimeSpan.FromSeconds(15));

            var removed = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = host.Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any();
            }, TimeSpan.FromSeconds(15));

            removed.ShouldBeTrue();

            OutboxDeduplicatableTestSubscriber.Received.ShouldHaveSingleItem();
            OutboxDeduplicatableTestSubscriber.Received.ShouldContain(message.Id);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Purges_Messages_Past_Retention_Period(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);

            options.UseOutbox<TestOutboxDbContext>(outbox =>
            {
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100));
                outbox.WithRetentionPeriod(TimeSpan.FromDays(30));
                outbox.WithPurgeInterval(TimeSpan.FromMilliseconds(100));
            });
        });

        using var host = builder.Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(host.Services);

        var staleMessageId = Guid.NewGuid();

        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();

            dbContext.OutboxMessages.Add(new OutboxMessage
            {
                Id = staleMessageId,
                MessageType = typeof(OutboxTestMessage).FullName!,
                Payload = [1, 2, 3],
                CreatedAt = DateTimeOffset.UtcNow.AddDays(-31),
            });

            await dbContext.SaveChangesAsync();
        }

        await host.StartAsync();

        try
        {
            var purged = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = host.Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any(message => message.Id == staleMessageId);
            }, TimeSpan.FromSeconds(15));

            purged.ShouldBeTrue();
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Dispatched_Message_Is_Removed_From_Database(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);

        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            configureDbContext(options, connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);

            options.UseOutbox<TestOutboxDbContext>(outbox =>
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100)));
        });

        using var host = builder.Build();

        await OutboxIntegrationTestContext.EnsureSchemaAsync(host.Services);

        using (var scope = host.Services.CreateScope())
        {
            var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
            dbContext.OutboxMessages.Add(new OutboxMessage
            {
                Id = Guid.NewGuid(),
                MessageType = typeof(OutboxTestMessage).FullName!,
                Payload = [1, 2, 3],
                CreatedAt = DateTimeOffset.UtcNow,
            });

            await dbContext.SaveChangesAsync();
        }

        await host.StartAsync();

        try
        {
            var removed = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = host.Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any();
            }, TimeSpan.FromSeconds(15));

            removed.ShouldBeTrue();
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Retains_Message_During_Broker_Outage_Then_Delivers_On_Recovery(OutboxDatabaseProvider provider)
    {
        using var host = await CreateAndStartOutboxHostAsync(
            provider,
            configureOutbox: outbox => outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100)),
            configure: options => options.Subscribe<OutboxTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50))));

        try
        {
            var message = new OutboxTestMessage { Guid = Guid.NewGuid() };

            await RabbitMqSharedFixture.PauseAsync();

            try
            {
                using (var scope = host.Services.CreateScope())
                {
                    var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                    var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                    await unitOfWork.ExecuteAsync(async ct => await publisher.PublishAsync(message, ct));
                }

                await Task.Delay(TimeSpan.FromSeconds(3));

                using (var scope = host.Services.CreateScope())
                {
                    var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                    var outboxCount = await dbContext.OutboxMessages.CountAsync();
                    outboxCount.ShouldBe(1);
                }

                OutboxTestSubscriber.Received.ShouldNotContain(message.Guid);
            }
            finally
            {
                await RabbitMqSharedFixture.UnpauseAsync();
            }

            (await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxTestSubscriber.Received.Contains(message.Guid),
                TimeSpan.FromSeconds(60))).ShouldBeTrue();

            var removed = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = host.Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any();
            }, TimeSpan.FromSeconds(15));

            removed.ShouldBeTrue();
            OutboxTestSubscriber.Received.ShouldContain(message.Guid);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [TestCase(OutboxDatabaseProvider.PostgreSql)]
    [TestCase(OutboxDatabaseProvider.SqlServer)]
    public async Task Outbox_Delivers_Multiple_Messages_In_Sequence_Order(OutboxDatabaseProvider provider)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);

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
                outbox.WithDispatchConcurrency(1);
            });

            options.Subscribe<OutboxOrderedTestSubscriber>(subscriber =>
            {
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50));
                subscriber.MaxMessageConcurrency(1);
            });
        });

        using var host = builder.Build();
        await OutboxIntegrationTestContext.EnsureSchemaAsync(host.Services);
        await host.StartAsync();

        try
        {
            for (var order = 1; order <= 5; order++)
            {
                using var scope = host.Services.CreateScope();
                var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                await unitOfWork.ExecuteAsync(async ct => await publisher.PublishAsync(
                    new OutboxOrderedTestMessage
                    {
                        Id = Guid.NewGuid(),
                        Order = order,
                    },
                    ct));
            }

            (await RabbitMqTestHelpers.WaitForAsync(
                () => OutboxOrderedTestSubscriber.GetReceivedOrder().Count == 5,
                TimeSpan.FromSeconds(15))).ShouldBeTrue();

            OutboxOrderedTestSubscriber.GetReceivedOrder().ShouldBe([1, 2, 3, 4, 5]);

            var removed = await RabbitMqTestHelpers.WaitForAsync(() =>
            {
                using var scope = host.Services.CreateScope();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                return !dbContext.OutboxMessages.Any();
            }, TimeSpan.FromSeconds(15));

            removed.ShouldBeTrue();
        }
        finally
        {
            await host.StopAsync();
        }
    }

    private static async Task<IHost> CreateAndStartOutboxHostAsync(
        OutboxDatabaseProvider provider,
        Action<OutboxOptions>? configureOutbox = null,
        Action<MessageServiceOptions>? configure = null)
    {
        var (connectionString, configureDbContext) = await OutboxIntegrationTestContext.CreateDatabaseAsync(provider);

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
