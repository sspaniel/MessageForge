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

public sealed class OutboxTests
{
    [SetUp]
    public void TestSetUp()
    {
        OutboxTestSubscriber.Reset();
        OutboxDeduplicatableTestSubscriber.Reset();
        OutboxOrderedTestSubscriber.Reset();
    }

    [Test]
    public async Task Outbox_Commits_Message_And_Service_Delivers_To_Subscriber()
    {
        // arrange
        var builder = Host.CreateApplicationBuilder();
        var connectionString = await PostgreSqlSharedFixture.CreateDatabaseConnectionStringAsync();

        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            options.UseNpgsql(connectionString));

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
        await EnsureSchemaAsync(host.Services);
        await host.StartAsync();

        try
        {
            var message = new OutboxTestMessage { Guid = Guid.NewGuid() };

            using (var scope = host.Services.CreateScope())
            {
                var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                await unitOfWork.BeginAsync();
                await publisher.PublishAsync(message);
                await unitOfWork.CommitAsync();
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

    [Test]
    public async Task Outbox_Deduplication_Skips_Duplicate_Pending_Messages()
    {
        // arrange
        var builder = Host.CreateApplicationBuilder();
        var connectionString = await PostgreSqlSharedFixture.CreateDatabaseConnectionStringAsync();

        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            options.UseNpgsql(connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);

            options.UseOutbox<TestOutboxDbContext>(outbox =>
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100)));

            options.Subscribe<OutboxDeduplicatableTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50)));
        });

        using var host = builder.Build();
        await EnsureSchemaAsync(host.Services);
        await host.StartAsync();

        try
        {
            var message = new OutboxDeduplicatableTestMessage { Id = Guid.NewGuid() };

            using (var scope = host.Services.CreateScope())
            {
                var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();

                await unitOfWork.BeginAsync();
                await publisher.PublishAsync(message);
                await publisher.PublishAsync(message);
                dbContext.OutboxMessages.Local.Count.ShouldBe(1);
                await unitOfWork.CommitAsync();
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

    [Test]
    public async Task Outbox_Dispatched_Message_Is_Removed_From_Database()
    {
        // arrange
        var builder = Host.CreateApplicationBuilder();
        var connectionString = await PostgreSqlSharedFixture.CreateDatabaseConnectionStringAsync();

        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            options.UseNpgsql(connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);

            options.UseOutbox<TestOutboxDbContext>(outbox =>
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100)));
        });

        using var host = builder.Build();

        await EnsureSchemaAsync(host.Services);

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

    [Test]
    public async Task Outbox_Rollback_Does_Not_Publish_Message()
    {
        // arrange
        using var host = await CreateAndStartOutboxHostAsync(
            configureOutbox: outbox => outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100)),
            configure: options => options.Subscribe<OutboxTestSubscriber>(subscriber =>
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50))));

        try
        {
            var message = new OutboxTestMessage { Guid = Guid.NewGuid() };

            using (var scope = host.Services.CreateScope())
            {
                var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                await unitOfWork.BeginAsync();
                await publisher.PublishAsync(message);
                await unitOfWork.RollbackAsync();
            }

            using (var scope = host.Services.CreateScope())
            {
                var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
                var outboxCount = await dbContext.OutboxMessages.CountAsync();
                outboxCount.ShouldBe(0);
            }

            await Task.Delay(TimeSpan.FromSeconds(3));
            OutboxTestSubscriber.Received.ShouldNotContain(message.Guid);
        }
        finally
        {
            await host.StopAsync();
        }
    }

    [Test]
    public async Task Outbox_Retains_Message_During_Broker_Outage_Then_Delivers_On_Recovery()
    {
        // arrange
        using var host = await CreateAndStartOutboxHostAsync(
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

                    await unitOfWork.BeginAsync();
                    await publisher.PublishAsync(message);
                    await unitOfWork.CommitAsync();
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

    [Test]
    public async Task Outbox_Delivers_Multiple_Messages_In_Sequence_Order()
    {
        // arrange
        var builder = Host.CreateApplicationBuilder();
        var connectionString = await PostgreSqlSharedFixture.CreateDatabaseConnectionStringAsync();

        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            options.UseNpgsql(connectionString));

        builder.Services.AddMessageForgeRabbitMQ(options =>
        {
            options.UseConnectionString(RabbitMqSharedFixture.ConnectionString);

            options.UseOutbox<TestOutboxDbContext>(outbox =>
            {
                outbox.WithPollingInterval(TimeSpan.FromMilliseconds(100));
                outbox.WithBatchSize(10);
            });

            options.Subscribe<OutboxOrderedTestSubscriber>(subscriber =>
            {
                subscriber.Retries(maxRetryCount: 3, retryDelay: TimeSpan.FromMilliseconds(50));
                subscriber.MaxMessageConcurrency(1);
            });
        });

        using var host = builder.Build();
        await EnsureSchemaAsync(host.Services);
        await host.StartAsync();

        try
        {
            for (var order = 1; order <= 5; order++)
            {
                using var scope = host.Services.CreateScope();
                var unitOfWork = scope.ServiceProvider.GetRequiredService<IUnitOfWork>();
                var publisher = scope.ServiceProvider.GetRequiredService<IPublisher>();

                await unitOfWork.BeginAsync();
                await publisher.PublishAsync(new OutboxOrderedTestMessage
                {
                    Id = Guid.NewGuid(),
                    Order = order,
                });
                await unitOfWork.CommitAsync();
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
        Action<OutboxOptions>? configureOutbox = null,
        Action<MessageServiceOptions>? configure = null)
    {
        var builder = Host.CreateApplicationBuilder();
        var connectionString = await PostgreSqlSharedFixture.CreateDatabaseConnectionStringAsync();

        builder.Services.AddDbContext<TestOutboxDbContext>(options =>
            options.UseNpgsql(connectionString));

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
        await EnsureSchemaAsync(host.Services);
        await host.StartAsync();
        return host;
    }

    private static async Task EnsureSchemaAsync(IServiceProvider services)
    {
        await using var scope = services.CreateAsyncScope();
        var dbContext = scope.ServiceProvider.GetRequiredService<TestOutboxDbContext>();
        await dbContext.Database.EnsureCreatedAsync();
    }
}
