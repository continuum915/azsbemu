using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using MassTransit;
using static AzsbEmu.Tests.IntegrationTests.MassTransitWait;

namespace AzsbEmu.Tests.IntegrationTests;

/// <summary>Placed by the transactional tests: the consumer publishes <see cref="OrderShipped"/> and fails when told to.</summary>
public record TransactionalOrder(string OrderId, bool Fail);

public record OrderShipped(string OrderId);

/// <summary>
/// Publishes OrderShipped in a Service Bus transaction, then fails (rolling it back) if the order says so.
/// The scope must flow across awaits: MassTransit's own <c>UseTransaction()</c> + <c>context.CreateTransactionScope()</c>
/// (8.5) does not, so the transaction is gone by the time MassTransit's send reaches the SDK and the publish is sent
/// outside it. That is MassTransit behaviour, the same against Azure; these tests use the pattern that works.
/// </summary>
public class ShipInTransactionConsumer : IConsumer<TransactionalOrder>
{
    public async Task Consume(ConsumeContext<TransactionalOrder> context)
    {
        using var scope = new System.Transactions.TransactionScope(System.Transactions.TransactionScopeAsyncFlowOption.Enabled);
        await context.Publish(new OrderShipped(context.Message.OrderId));
        if (context.Message.Fail) throw new InvalidOperationException($"failing {context.Message.OrderId} after publishing");
        scope.Complete();
    }
}

/// <summary>
/// Spec story 12: a MassTransit consumer publishing inside a Service Bus transaction (enlisted by the SDK through the
/// emulator's <c>$coordinator</c>). On commit the message goes through MassTransit's forwarding topology (topic →
/// subscription → queue) exactly once; on rollback it is never delivered. Both with and without the SDK's cross-entity
/// transactions.
/// </summary>
public class MassTransitTransactionTests : IDisposable
{
    private readonly BrokerFixture _broker = new();

    public void Dispose() => _broker.Dispose();

    private IBusControl CreateBus(ConcurrentQueue<OrderShipped> shipped, bool crossEntityTransactions)
    {
        var client = new ServiceBusClient(_broker.ConnectionString, new ServiceBusClientOptions { EnableCrossEntityTransactions = crossEntityTransactions });
        return Bus.Factory.CreateUsingAzureServiceBus(cfg =>
        {
            cfg.Host(new Uri("sb://localhost/"), client, _broker.CreateAdministrationClient());
            cfg.ReceiveEndpoint("orders-tx", e => e.Consumer<ShipInTransactionConsumer>());
            cfg.ReceiveEndpoint("shipping-tx", e => e.Consumer(() => new RecordingConsumer<OrderShipped>(shipped)));
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task APublishInACommittedTransaction_IsDeliveredOnce_ThroughForwarding(bool crossEntityTransactions)
    {
        var shipped = new ConcurrentQueue<OrderShipped>();
        var bus = CreateBus(shipped, crossEntityTransactions);
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            await bus.Publish(new TransactionalOrder("committed", Fail: false));

            await UntilAsync(() => shipped.Count == 1, "the order shipped in the committed transaction");
            await Task.Delay(1000);
            Assert.Equal(new[] { "committed" }, shipped.Select(s => s.OrderId));
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task APublishInARolledBackTransaction_IsNeverDelivered(bool crossEntityTransactions)
    {
        var shipped = new ConcurrentQueue<OrderShipped>();
        var bus = CreateBus(shipped, crossEntityTransactions);
        await bus.StartAsync(NewTimeoutToken());
        try
        {
            await bus.Publish(new TransactionalOrder("rolled-back", Fail: true));
            await bus.Publish(new TransactionalOrder("committed-after", Fail: false));

            // The failed order reached its error queue, so its transaction has ended; the next order's publish arrived,
            // so the rolled-back one would have arrived by now too.
            var admin = _broker.CreateAdministrationClient();
            await UntilAsync(async () => (await admin.QueueExistsAsync("orders-tx_error")).Value
                                         && _broker.GetQueue("orders-tx_error").MessageCount == 1, "the failed order in orders-tx_error");
            await UntilAsync(() => shipped.Any(s => s.OrderId == "committed-after"), "the order after the rollback");
            await Task.Delay(1000);

            Assert.DoesNotContain(shipped, s => s.OrderId == "rolled-back");
            Assert.Single(shipped);
        }
        finally
        {
            await bus.StopAsync(NewTimeoutToken());
        }
    }
}
