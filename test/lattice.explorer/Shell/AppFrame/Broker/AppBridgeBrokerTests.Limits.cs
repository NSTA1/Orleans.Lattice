using Orleans.Lattice.Explorer.Shell.Framing.Broker;
using static Orleans.Lattice.Explorer.Tests.Shell.Framing.Broker.BrokerHarness;

namespace Orleans.Lattice.Explorer.Tests.Shell.Framing.Broker;

/// <summary>The per-frame token bucket and concurrency limit, driven by a manual clock.</summary>
public sealed partial class AppBridgeBrokerTests
{
    [Test]
    public async Task A_burst_beyond_the_bucket_is_rate_limited_until_time_refills_it()
    {
        var harness = await CreateAsync();

        for (var id = 1; id <= AppBridgeRateLimiter.Capacity; id++)
        {
            AssertOk(await harness.SendAsync(id, "context.read"), id);
        }

        var limited = await harness.SendAsync(100, "data.read", OrdersGet);
        AssertRefused(limited, 100, "rate_limited");
        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge!.Calls, Is.Empty);
            Assert.That(harness.Log.Messages, Has.Some.Contains("RateLimited"));
        });

        harness.Time.Advance(TimeSpan.FromSeconds(1));
        for (var id = 200; id < 200 + (int)AppBridgeRateLimiter.RefillPerSecond; id++)
        {
            AssertOk(await harness.SendAsync(id, "context.read"), id);
        }

        AssertRefused(await harness.SendAsync(300, "context.read"), 300, "rate_limited");
    }

    [Test]
    public async Task The_bucket_never_refills_above_its_capacity()
    {
        var harness = await CreateAsync();
        harness.Time.Advance(TimeSpan.FromHours(1));

        for (var id = 1; id <= AppBridgeRateLimiter.Capacity; id++)
        {
            AssertOk(await harness.SendAsync(id, "context.read"), id);
        }

        AssertRefused(await harness.SendAsync(99, "context.read"), 99, "rate_limited");
    }

    [Test]
    public async Task A_fifth_concurrent_request_is_refused_and_a_slot_frees_on_completion()
    {
        var harness = await CreateAsync();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Bridge!.Gate = gate;

        var inFlight = Enumerable.Range(1, AppBridgeRateLimiter.MaxInFlight)
            .Select(id => harness.SendAsync(id, "data.read", OrdersGet))
            .ToArray();
        Assert.That(harness.Session.Limiter.InFlight, Is.EqualTo(AppBridgeRateLimiter.MaxInFlight));

        var fifth = await harness.SendAsync(5, "data.read", OrdersGet);
        AssertRefused(fifth, 5, "rate_limited");
        Assert.Multiple(() =>
        {
            Assert.That(harness.Bridge.Calls, Has.Count.EqualTo(AppBridgeRateLimiter.MaxInFlight));
            Assert.That(harness.Log.Messages, Has.Some.Contains("ConcurrencyLimited"));
        });

        gate.SetResult();
        await Task.WhenAll(inFlight);
        harness.Bridge.Gate = null;

        Assert.That(harness.Session.Limiter.InFlight, Is.Zero);
        AssertOk(await harness.SendAsync(6, "data.read", OrdersGet), 6);
    }

    [Test]
    public async Task A_refused_request_releases_its_slot()
    {
        var harness = await CreateAsync();
        harness.Bridge!.Throw = new InvalidOperationException();

        await harness.SendAsync(1, "data.read", OrdersGet);

        Assert.That(harness.Session.Limiter.InFlight, Is.Zero);
    }

    [Test]
    public async Task Each_frame_has_its_own_budget()
    {
        var harness = await CreateAsync();
        for (var id = 1; id <= AppBridgeRateLimiter.Capacity; id++)
        {
            await harness.SendAsync(id, "context.read");
        }

        var second = harness.Broker.Open(harness.Session.Launch);

        AssertOk(await harness.Broker.HandleAsync(second, "{\"id\":1,\"op\":\"context.read\",\"args\":{}}"), 1);
    }

    [Test]
    public void AppBridgeRateLimiter_rejects_a_null_clock_and_exit_never_goes_negative()
    {
        var limiter = new AppBridgeRateLimiter(new ManualTimeProvider());
        limiter.Exit();

        Assert.Multiple(() =>
        {
            Assert.That(() => new AppBridgeRateLimiter(null!), Throws.ArgumentNullException);
            Assert.That(limiter.InFlight, Is.Zero);
        });
    }
}
