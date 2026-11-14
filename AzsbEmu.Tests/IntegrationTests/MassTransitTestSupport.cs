using System.Collections.Concurrent;
using MassTransit;

namespace AzsbEmu.Tests.IntegrationTests;

// Messages and consumers shared by the MassTransit test classes.

public record OrderPlaced(string OrderId);

public record ShipOrder(string OrderId);

public record PriceQuery(string Sku);

public record PriceQuote(string Sku, decimal Price);

public class RecordingConsumer<T>(ConcurrentQueue<T> received) : IConsumer<T> where T : class
{
    public Task Consume(ConsumeContext<T> context)
    {
        received.Enqueue(context.Message);
        return Task.CompletedTask;
    }
}

public class ThrowingConsumer : IConsumer<OrderPlaced>
{
    public Task Consume(ConsumeContext<OrderPlaced> context) => throw new InvalidOperationException("cannot place order");
}

public class PricingConsumer : IConsumer<PriceQuery>
{
    public Task Consume(ConsumeContext<PriceQuery> context) => context.RespondAsync(new PriceQuote(context.Message.Sku, 9.99m));
}

internal static class MassTransitWait
{
    /// <summary>A fresh 30-second cancellation token for bus start/stop.</summary>
    public static CancellationToken NewTimeoutToken() => new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token;

    /// <summary>Polls <paramref name="condition"/> until it holds, failing the test after 15 seconds.</summary>
    public static async Task UntilAsync(Func<Task<bool>> condition, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail($"Timed out waiting for {what}.");
            await Task.Delay(50);
        }
    }

    public static Task UntilAsync(Func<bool> condition, string what) => UntilAsync(() => Task.FromResult(condition()), what);
}
